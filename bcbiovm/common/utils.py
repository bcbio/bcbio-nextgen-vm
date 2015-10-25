"""Utilities and helper functions."""
# pylint: disable=too-many-arguments

import collections
import contextlib
import datetime
import os
import shutil
import subprocess
import sys
import tarfile
import time

import paramiko
import six

from bcbiovm import config as global_config
from bcbiovm.common import constant

_SYMBOLS = {
    'customary_symbols': ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_names': ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta',
                        'exa', 'zetta', 'iotta'),
    # Note(alexandrucoman): More information can be found on the following link
    #                       http://goo.gl/uyQruU
    'IEC_symbols': ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'IEC_names': ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi',
                  'zebi', 'yobi'),
}


class SSHAgent(object):

    """Minimalist wrapper over `ssh-agent`."""

    def __init__(self, keys):
        self._keys = keys

    def __enter__(self):
        """Setup the SSH Agent at the beginning of the block created
        by the with statement.

        :raise:
        """
        output, _ = execute(['ssh-agent', '-s'], check_exit_code=True)
        for line in output.splitlines():
            key, _, value = line.partition('=')
            if not value:
                continue
            value = value.split(';')[0]
            os.environ[key] = value

        for key_path in self._keys:
            execute(['ssh-add', key_path])

    def __exit__(self, exception_type, exception_value, traceback):
        execute(['ssh-agent', '-k'])


class SSHClient(object):

    """Wrapper over paramiko SHH client."""

    def __init__(self, host=constant.SSH.HOST, port=constant.SSH.PORT,
                 user=constant.SSH.USER):
        """
        :param host:    the server to connect to
        :param port:    the server port to connect to
        :param user:    the username to authenticate as (defaults to
                        the current local username)
        """
        self._host = host
        self._port = port
        self._user = user
        self._ssh_client = paramiko.client.SSHClient()

    @property
    def client(self):
        """SSH Client."""
        return self._ssh_client

    def connect(self, bastion_host=None, user='ec2-user'):
        """Connect to an SSH server and authenticate to it.
        :param bastion_host:  the bastion host to connect to

        Note:
            In order to connect from the bastion host to another instance
            without storing the private key on the bastion SSH tunneling
            will be used. More information can be found on the following
            link: http://goo.gl/wqkHEk
        """
        proxy_command = None
        if bastion_host:
            # NOTE(alexandrucoman): Avoid pylint FP
            # pylint: disable=no-member
            proxy_command = paramiko.proxy.ProxyCommand(
                constant.SSH.PROXY % {"host": self._host,
                                      "port": self._port,
                                      "user": user,
                                      "bastion": bastion_host}
            )

        try:
            # NOTE(alexandrucoman): Avoid pylint FP
            # pylint: disable=unexpected-keyword-arg
            self._ssh_client.connect(self._host, username=self._user,
                                     allow_agent=True, sock=proxy_command)
        except paramiko.SSHException:
            # FIXME(alexandrucoman): Raise custom exception
            pass

    def close(self):
        """Close this SSHClient and its underlying Transport."""
        self._ssh_client.close()

    def execute(self, command):
        """Execute a command on the SSH server.

        :param command:   the command to execute
        """
        command = " ".join([str(argument) for argument in command])
        try:
            _, stdout, _ = self._ssh_client.exec_command(command)
        except paramiko.SSHException:
            # FIXME(alexandrucoman): Treat properly this exception
            return

        return stdout.read()

    def download_file(self, source, destination, **kwargs):
        """Download the source file to the received destination.

        :param source:      the path of the file which should be downloaded
        :param destination: the path where the file should be written
        :param permissions: The octal permissions set that should be given for
                            this file.
        :param open_mode:   The open mode used when opening the file.
        :param utime:       2-tuple of numbers, of the form (atime, mtime)
                            which is used to set the access and modified times
        """
        output = self.execute(['cat', source])
        write_file(destination, output, *kwargs)

    def stat(self, path, stat_format=("%s", "%Y", "%n")):
        """Return the detailed status of a particular file or a file system.

        :param path:          path to a file or a file system
        :param stat_format:   a valid format sequences
        """
        file_status = []
        format_string = '"%(format)s"' % {"format": '|'.join(stat_format)}
        output = self.execute(['stat', '--format', format_string, path])
        if not output:
            # FIXME(alexandrucoman): Treat properly this branch
            return None

        for line in output.splitlines():
            if '|' not in line:
                continue
            file_status.append(output.split('|'))

        return file_status

    def disk_space(self, path, ftype=None):
        """Return the amount of disk space available on the file system
        containing the received file.

        :param ftype:   limit listing to file systems of the received type.
        :path path:     the path of the file

        :return:        a namedtuple which contains the following fields:
                        filesystem, total, used, available, percentage and
                        mount_point
        """
        output = []
        df_output = collections.namedtuple(
            "DiskSpace", ["filesystem", "total", "used", "available",
                          "percentage", "mount_point"])
        command = ["df"]
        if ftype:
            command.extend(["-t", ftype])
        command.extend(path)

        output = self.execute(command)
        if not output:
            # FIXME(alexandrucoman): Treat properly this branch
            return None

        # Ignore the first row from df output (the table header)
        for file_system in output.splitlines()[:1]:
            output.append(df_output(file_system.split()))

        return output


def write_file(path, content, permissions=0o644, open_mode="wb", utime=None):
    """Writes a file with the given content.

    Also the function sets the file mode as specified.

    :param path:        The absolute path to the location on the filesystem
                        wherethe file should be written.
    :param content:     The content that should be placed in the file.
    :param permissions: The octal permissions set that should be given for
                        this file.
    :param open_mode:   The open mode used when opening the file.
    :param utime:       2-tuple of numbers, of the form (atime, mtime) which
                        is used to set the access and modified times
    """
    dirname = os.path.dirname(path)
    if not os.path.isdir(dirname):
        try:
            os.makedirs(dirname)
        except OSError:
            return False

    with open(path, open_mode) as file_handle:
        file_handle.write(content)
        file_handle.flush()

    os.chmod(path, permissions)
    os.utime(path, utime)
    return True


def execute(command, **kwargs):
    """Helper method to shell out and execute a command through subprocess.

    :param attempts:        How many times to retry running the command.
    :param binary:          On Python 3, return stdout and stderr as bytes if
                            binary is True, as Unicode otherwise.
    :param check_exit_code: Single bool, int, or list of allowed exit
                            codes.  Defaults to [0].  Raise
                            :class:`CalledProcessError` unless
                            program exits with one of these code.
    :param command:         The command passed to the subprocess.Popen.
    :param cwd:             Set the current working directory
    :param env_variables:   Environment variables and their values that
                            will be set for the process.
    :param retry_interval:  Interval between execute attempts, in seconds
    :param shell:           whether or not there should be a shell used to
                            execute this command.

    :raises:                :class:`subprocess.CalledProcessError`
    """
    # pylint: disable=too-many-locals

    attempts = kwargs.pop("attempts", global_config.misc["attempts"])
    binary = kwargs.pop('binary', False)
    check_exit_code = kwargs.pop('check_exit_code', [0])
    cwd = kwargs.pop('cwd', None)
    env_variables = kwargs.pop("env_variables", None)
    retry_interval = kwargs.pop("retry_interval",
                                global_config.misc["retry_interval"])
    shell = kwargs.pop("shell", False)

    command = [str(argument) for argument in command]
    ignore_exit_code = False

    if isinstance(check_exit_code, bool):
        ignore_exit_code = not check_exit_code
        check_exit_code = [0]
    elif isinstance(check_exit_code, int):
        check_exit_code = [check_exit_code]

    while attempts > 0:
        attempts = attempts - 1
        try:
            process = subprocess.Popen(command,
                                       stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, shell=shell,
                                       cwd=cwd, env=env_variables)
            result = process.communicate()
            return_code = process.returncode

            if six.PY3 and not binary and result is not None:
                # pylint: disable=no-member

                # Decode from the locale using using the surrogate escape error
                # handler (decoding cannot fail)
                (stdout, stderr) = result
                stdout = os.fsdecode(stdout)
                stderr = os.fsdecode(stderr)
            else:
                stdout, stderr = result

            if not ignore_exit_code and return_code not in check_exit_code:
                raise subprocess.CalledProcessError(returncode=return_code,
                                                    cmd=command,
                                                    output=(stdout, stderr))
            else:
                return (stdout, stderr)
        except subprocess.CalledProcessError:
            if attempts:
                time.sleep(retry_interval)
            else:
                raise

    # TODO(alexandrucoman): Raise BCBioException or another custom exception:
    #                       The maximum number of attempts has been exceeded.


def write_elasticluster_config(config, output,
                               provider=constant.DEFAULT_PROVIDER):
    """Write Elasticluster configuration file with user and security
    information.
    """
    template_file = os.path.join(
        sys.prefix, "share", "bcbio-vm", "elasticluster",
        "{provider}.config".format(provider=provider))
    content = []

    if not os.path.exists(os.path.dirname(output)):
        os.makedirs(os.path.dirname(output))

    if os.path.exists(output):
        now = datetime.datetime.now()
        backup_file = ("%(base)s.bak%(timestamp)s" %
                       {"base": output,
                        "timestamp": now.strftime("%Y-%m-%d-%H-%M-%S")})
        shutil.move(output, backup_file)

    with open(template_file, "r") as file_handle:
        for line in file_handle.readlines():
            line = line.strip()
            key, sep, _ = line.partition("=")
            if sep == "=" and key in config:
                content.append("%(name)s=%(value)s" %
                               {"name": key, "value": config[key]})
            else:
                content.append(line)

    write_file(output, "\n".join(content), open_mode="w")


def backup(path, backup_dir=None, delete=False, maximum=0):
    """
    Make a copy of the received file.

    :param path:     The absolute path of the file.
    :param backup:   The absolute path to the location on the filesystem
                     wherethe file should be written.
    :param delete:   Delete the source file.
    :param maximum:  The maximum number of backup files allowed.
    """
    backup_files = []
    now = datetime.datetime.now()
    template = "{filename}_{timestamp}.bak"

    path = os.path.abspath(path)
    filename = os.path.basename(path)
    backup_dir = backup_dir or os.path.dirname(path)
    action = shutil.move if delete else shutil.copy

    if maximum > 0:
        for backup_file in os.listdir(backup_dir):
            source, _ = backup_file.rsplit("_", 1)
            if backup_file.endswith(".bak") and filename == source:
                backup_files.append(backup_file)

        backup_files.sort()
        for backup_file in backup_files[maximum - 1:]:
            os.remove(backup_file)

    filename = template.format(filename=filename,
                               timestamp=now.strftime("%Y-%m-%d-%H-%M-%S"))
    action(path, os.path.join(backup_dir, filename))


def predict_unit(unit):
    """Predit the symbol set for the received unit."""
    symbol_value = 1
    _symbol = collections.namedtuple("Symbol", ["name", "set", "value"])

    for set_name, symbol_set in _SYMBOLS.items():
        if unit in symbol_set:
            break
    else:
        if unit == 'k':
            # Treat `k` as an alias for `K`
            set_name = "customary_symbols"
            symbol_set = _SYMBOLS["customary_symbols"]
            unit = unit.upper()
        else:
            raise ValueError("Invalid unit name %(unit)s", {"unit": unit})

    if unit != symbol_set[0]:
        symbol_value = 1 << symbol_set.index(unit) * 10

    return _symbol(set_name, symbol_set, symbol_value)


def predict_size(size, convert="K"):
    """Attempts to guess the string format based on default symbols
    set and return the corresponding bytes as an integer.
    """
    initial_size = size.strip()
    numerical = ""
    while (initial_size and initial_size[0:1].isdigit() or
           initial_size[0:1] == '.'):
        numerical += initial_size[0]
        initial_size = initial_size[1:]
    numerical = float(numerical)

    symbol = predict_unit(initial_size.strip())
    new_symbol = predict_unit(convert)
    return int(numerical * symbol.value) / new_symbol.value


def compress(source, destination=None, compression="gz"):
    """Saves many files together into a single tape or disk archive,
    and can restore individual files from the archive.

    :param source:      the path of the files that will be saved together
    :param destination: the path of the output
    :param compression: the compression level of the file

    :raises:
        If a compression method is not supported, CompressionError is raised.
    """
    open_mode = "w:{0}".format(compression) if compression else "w"
    source = source if isinstance(source, (list, tuple)) else (source, )
    destination = destination or source[0].join((".tar", compression))

    with contextlib.closing(tarfile.open(destination, open_mode)) as archive:
        for path in source:
            archive.add(path, arcname=os.path.basename(path))

    return destination


def upgrade_bcbio_vm():
    """Upgrade bcbio-nextgen-vm wrapper code."""
    executable = os.path.dirname(os.path.realpath(sys.executable))
    conda_bin = os.path.join(executable, "conda")
    if not os.path.exists(conda_bin):
        return False

    execute([conda_bin, "install", "--yes",
             "-c", global_config.conda["channel"],
             global_config.conda["package"]], check_exit_code=0)

    return True
