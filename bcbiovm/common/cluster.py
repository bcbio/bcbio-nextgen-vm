"""Manage a cluster's life cycle."""

import os
import sys

import ansible.utils
import ansible.callbacks
import ansible.playbook
import elasticluster
import voluptuous

from bcbiovm.common import constant
from bcbiovm.common import utils

LOG = utils.get_logger(__name__)
__all__ = ['AnsiblePlaybook', 'ElastiCluster']


class ElastiCluster(object):

    """Wrapper over the elasticluster functionalities."""

    _CONFIG = ("-c", "--config")
    _STORAGE = ("-s", "--storage")
    _VERBOSE = ("-v", "--verbose")
    _FORCE = "--force"
    _NO_SETUP = "--no-setup"
    _USE_DEFAULTS = "--yes"
    _EC = "elasticluster"
    _EC_START = "start"
    _EC_STOP = "stop"
    _EC_SSH = "ssh"
    _EC_SETUP = "setup"

    def __init__(self, config):
        self._config = None
        self._config_file = config

        self._load_config()

    @property
    def config(self):
        """Instance of :class Configurator:."""
        return self._config

    def _load_config(self):
        """Load the Elasticluster configuration."""
        # TODO(alexandrucoman): Change `storage` with a constant
        storage_dir = os.path.join(os.path.dirname(self._config_file),
                                   "storage")
        try:
            self._config = elasticluster.conf.Configurator.fromConfig(
                self._config_file, storage_dir)
        except voluptuous.Error:
            # FIXME(alexandrucoman): Raise InvalidConfig
            return None

    def get_config(self, cluster_name=None):
        """Get the config."""
        if not cluster_name:
            return self._config

        if cluster_name in self._config.cluster_conf:
            return self._config.cluster_conf[cluster_name]

        # FIXME(alexandrucoman): Raise InvalidCluster exception
        return None

    def get_cluster(self, cluster_name):
        """Loads a cluster from the cluster repository.

        :param cluster_name: name of the cluster
        :return: :class elasticluster.cluster.cluster: instance
        """
        return self._config.load_cluster(cluster_name)

    @classmethod
    def _add_common_options(cls, command, config=None, verbose=None):
        """Add common options to the command line."""
        if config:
            # Add `--config config_file` to the command
            command.extend([cls._CONFIG[1], config])

        if verbose:
            # Add `--verbose` to the command
            command.append(cls._VERBOSE[1])

    @classmethod
    def _check_command(cls, command):
        """Check if all the required information are present in
        the command line.

        Note:
            If the storage or the config is missing they will be added.
        """
        # Ckeck if received command contains '-s' or '--storage'
        if cls._STORAGE[0] not in command and cls._STORAGE[1] not in command:
            # Insert `--storage storage_path` to the command
            for argument in (constant.PATH.EC_STORAGE, cls._STORAGE[1]):
                command.insert(1, argument)

            # Notes: Clean up the old storage directory in order to avoid
            #        consistent errors.
            std_args = [arg for arg in command if not arg.startswith('-')]
            if len(std_args) >= 3 and std_args[1] == "start":
                pickle_file = (constant.PATH.PICKLE_FILE %
                               {"cluster": std_args[2]})
                if os.path.exists(pickle_file):
                    os.remove(pickle_file)

        # Check if received command contains '-c' or '--config'
        if cls._CONFIG[0] not in command and cls._CONFIG[1] not in command:
            # Insert `--config config_file` to the command
            for argument in (constant.PATH.EC_CONFIG, cls._CONFIG[1]):
                command.insert(1, argument)

    @classmethod
    def execute(cls, command, **kwargs):
        """Wrap elasticluster commands to avoid need to call separately."""
        # Note: Sets NFS client parameters for elasticluster Ansible playbook.
        #       Uses async clients which provide better throughput on
        #       reads/writes: http://goo.gl/tGrGtE (section 5.9 for tradeoffs)
        os.environ["nfsoptions"] = constant.NFS_OPTIONS
        cls._add_common_options(command, **kwargs)
        cls._check_command(command)
        sys.argv = command
        try:
            return elasticluster.main.main()
        except SystemExit as exc:
            return exc.args[0]

    @classmethod
    def start(cls, cluster, config=None, no_setup=False, verbose=False):
        """Create a cluster using the supplied configuration.

        :param cluster:   Type of cluster. It refers to a
                          configuration stanza [cluster/<name>].
        :param config:    Elasticluster config file
        :param no_setup:  Only start the cluster, do not configure it.
        :param verbose:   Increase verbosity.
        """
        command = [cls._EC, cls._EC_START, cluster]
        if no_setup:
            command.append(cls._NO_SETUP)

        return cls.execute(command, config=config, verbose=verbose)

    @classmethod
    def stop(cls, cluster, config=None, force=False, use_default=False,
             verbose=False):
        """Stop a cluster and all associated VM instances.

        :param cluster:     Type of cluster. It refers to a
                            configuration stanza [cluster/<name>].
        :param config:      Elasticluster config file
        :param force:       Remove the cluster even if not all the nodes
                            have been terminated properly.
        :param use_default: Assume `yes` to all queries and do not prompt.
        :param verbose:     Increase verbosity.
        """
        command = [cls._EC, cls._EC_STOP, cluster]
        if force:
            command.append(cls._FORCE)
        if use_default:
            command.append(cls._USE_DEFAULTS)

        return cls.execute(command, config=config, verbose=verbose)

    @classmethod
    def setup(cls, cluster, config=None, verbose=False):
        """Configure the cluster.

        :param cluster:     Type of cluster. It refers to a
                            configuration stanza [cluster/<name>].
        :param config:      Elasticluster config file
        :param verbose:     Increase verbosity.
        """
        command = [cls._EC, cls._EC_SETUP, cluster]
        return cls.execute(command, config=config, verbose=verbose)

    @classmethod
    def ssh(cls, cluster, config=None, ssh_args=None, verbose=False):
        """Connect to the frontend of the cluster using the `ssh` command.

        :param cluster:     Type of cluster. It refers to a
                            configuration stanza [cluster/<name>].
        :param config:      Elasticluster config file
        :ssh_args:          SSH command.
        :param verbose:     Increase verbosity.

        Note:
            If the ssh_args are provided the command will be executed on
            the remote machine instead of opening an interactive shell.
        """
        command = [cls._EC, cls._EC_SSH, cluster]
        if ssh_args:
            command.extend(ssh_args)
        return cls.execute(command, config=config, verbose=verbose)


class SilentPlaybook(ansible.callbacks.PlaybookCallbacks):

    """
    Suppress Ansible output when running playbooks.
    """
    # TODO(alexandrucoman): Add debug messages for each callback
    #                       overwritten below.

    def on_no_hosts_matched(self):
        """Callback for `no_hosts_matched` event."""
        pass

    def on_no_hosts_remaining(self):
        """Callback for `no_hosts_remaining` event."""
        pass

    def on_task_start(self, name, is_conditional):
        """Callback for `task_start` event."""
        pass

    def on_setup(self):
        """Callback for `setup` event."""
        pass

    def on_import_for_host(self, host, imported_file):
        """Callback for `import_for_host` event."""
        pass

    def on_not_import_for_host(self, host, missing_file):
        """Callback for `not_import_for_host` event."""
        pass

    def on_play_start(self, pattern):
        """Callback for `play_start` event."""
        pass

    def on_stats(self, stats):
        """Callback for `stats` event."""
        pass


class AnsiblePlaybook(object):

    """
    Generalized functionality for running an ansible playbook on
    elasticluster.
    """

    CONFIG = 'ANSIBLE_CONFIG'
    TEMP_CONFIG = 'ANSIBLE_TEMP_CONFIG'
    HOST_KEY_CHECKING = 'ANSIBLE_HOST_KEY_CHECKING'

    def __init__(self, inventory_path, playbook_path, args, extra_vars=None,
                 ansible_cfg=None):
        """
        :param inventory_path:  the path to the inventory hosts file
        :param playbook_path:   the path to a playbook file
        :param args:            arguments received from the client
        :param extra_args:      is an option function that should return
                                extra variables to pass to ansible given
                                the arguments and cluster configuration
        """
        self._host_list = inventory_path
        self._playbook = playbook_path
        self._ansible_cfg = ansible_cfg

        self._callbacks = None
        self._cluster = None
        self._stats = None
        self._runner_cb = None

        if hasattr(args, "econfig") and hasattr(args, "cluster"):
            ecluster = ElastiCluster(args.econfig)
            self._cluster = ecluster.get_config(args.cluster)

        self._extra_vars = self._get_extra_vars(args, extra_vars)
        self._setup(args)

    def _setup(self, args):
        """Setup all the requirements for the playbook.

        :param args:        arguments received from the client
        """
        self._stats = ansible.callbacks.AggregateStats()
        if args.verbose:
            self._callbacks = ansible.callbacks.PlaybookCallbacks()
            self._runner_cb = ansible.callbacks.PlaybookRunnerCallbacks(
                self._stats)
            ansible.utils.VERBOSITY = args.verbose - 1
        else:
            self._callbacks = SilentPlaybook()
            self._runner_cb = ansible.callbacks.DefaultRunnerCallbacks()

    def _get_extra_vars(self, args, extra_vars):
        """Return variables which need to be injected into playbook
        if they exist.

        :param args:        arguments received from the client
        :param extra_vars:  is an option function that should return
                            extra variables to pass to ansible given
                            the arguments and cluster configuration
        """
        if extra_vars and self._cluster:
            return extra_vars(args, self._cluster)

        return {}

    def prologue(self):
        """Setup the environment before playbook run."""
        os.environ[self.HOST_KEY_CHECKING] = constant.ANSIBLE.KEY_CHECKING
        if self._ansible_cfg:
            os.environ[self.TEMP_CONFIG] = os.environ.get(self.CONFIG)
            os.environ[self.CONFIG] = self._ansible_cfg
            reload(ansible.constants)

    def epilogue(self):
        """Cleanup the environment after playbook run."""
        if self._ansible_cfg:
            old_ansible_cfg = os.environ.pop(self.TEMP_CONFIG, None)
            if old_ansible_cfg:
                os.environ[self.CONFIG] = old_ansible_cfg
            else:
                del os.environ[self.CONFIG]
            reload(ansible.constants)

    def _run(self):
        """Run the playbook.

        :return: A tuple with two dictionaries. The first dictionary
                 contains information regarding unreachable hosts and
                 the second one contains information regarding failures.
        """
        private_key = None
        if self._cluster:
            private_key = self._cluster['login']['user_key_private']

        playbook = ansible.playbook.PlayBook(
            playbook=self._playbook,
            module_path=constant.PATH.EC_ANSIBLE_LIBRARY,
            extra_vars=self._extra_vars,
            host_list=self._host_list,
            private_key_file=private_key,
            callbacks=self._callbacks,
            runner_callbacks=self._runner_cb,
            forks=constant.ANSIBLE.FORKS,
            stats=self._stats
        )
        status = playbook.run()

        unreachable = []
        failures = {}
        for host, hoststatus in status.items():
            if hoststatus['unreachable']:
                unreachable.append(host)
            if hoststatus['failures']:
                failures[host] = hoststatus['failures']

        return (unreachable, failures)

    def run(self):
        """Generalized functionality for running an ansible playbook on
        elasticluster.

        :return: A tuple with two dictionaries. The first dictionary
                 contains information regarding unreachable hosts and
                 the second one contains information regarding failures.
        """
        self.prologue()
        response = self._run()
        self.epilogue()

        return response
