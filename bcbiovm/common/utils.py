"""Utilities and helper functions."""
# pylint: disable=too-many-arguments

import collections
import datetime
import logging
import os
import shutil
import subprocess
import sys
import time

import paramiko
import six

from bcbiovm.common import constant


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


def get_logger(name=constant.LOG.NAME, format_string=None):
    """Obtain a new logger object.

    :param name:          the name of the logger
    :param format_string: the format it will use for logging.

    If it is not given, the the one given at command
    line will be used, otherwise the default format.
    """
    logger = logging.getLogger(name)
    formatter = logging.Formatter(
        format_string or constant.LOG.FORMAT)

    if not logger.handlers:
        # If the logger wasn't obtained another time,
        # then it shouldn't have any loggers

        if constant.LOG.FILE:
            file_handler = logging.FileHandler(constant.LOG.FILE)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        logger.addHandler(stdout_handler)

    logger.setLevel(constant.LOG.LEVEL)
    return logger


def write_file(path, content, permissions=constant.DEFAULT_PERMISSIONS,
               open_mode="wb", utime=None):
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
    os.utime(utime)
    return True


def execute(*command, **kwargs):
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

    attempts = kwargs.pop("attempts", constant.MISC.ATTEMPTS)
    binary = kwargs.pop('binary', False)
    check_exit_code = kwargs.pop('check_exit_code', [0])
    cwd = kwargs.pop('cwd', None)
    env_variables = kwargs.pop("env_variables", None)
    retry_interval = kwargs.pop("retry_interval", constant.MISC.RETRY_INTERVAL)
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
            return_code = result.returncode     # pylint: disable=no-member

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
    template_file = constant.PATH.EC_CONFIG_TEMPLATE.format(provider=provider)
    config_file = {}
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
            key, sep, value = line.partition("=")
            if sep != "=":
                continue
            config[key] = value

    config_file.update(config)
    for key, value in config_file.items():
        content.append("%(name)s=%(value)s" % {"name": key, "value": value})
    write_file(output, "\n".join(content), open_mode="w")
