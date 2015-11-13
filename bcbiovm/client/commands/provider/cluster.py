"""Run and manage a cluster using elasticluster."""
from __future__ import print_function

import abc
import argparse

import six

from bcbiovm import log as logging
from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.common import objects
from bcbiovm.common import utils as common_utils
from bcbiovm.provider import factory as cloud_factory

LOG = logging.get_logger(__name__)


class CommandMixin(base.Command):

    """Base command class for commands which are ussing AnsiblePlaybook."""

    @staticmethod
    def _process_playbook_response(response):
        """Process the information received from AnsiblePlaybook."""
        status = True
        report = objects.Report()
        fields = [
            {"name": "playbook", "title": "Ansible playbook"},
            {"name": "unreachable", "title": "Unreachable host"},
            {"name": "failures", "title": "Hosts where playbook failed."}
        ]

        for playbook, playbook_info in response.items():
            if not playbook_info.status:
                status = False

            section = report.add_section(
                name=playbook, fields=fields,
                title="Ansible playbook: %s" % playbook)
            section.add_item([playbook, playbook_info.unreachable,
                              playbook_info.failures])

        return(status, report)

    def prologue(self):
        """Executed once before the command running."""
        super(CommandMixin, self).prologue()
        if self.args.econfig is None:
            self.args.econfig = constant.PATH.EC_CONFIG.format(
                provider=self.args.provider)

    @abc.abstractmethod
    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        pass

    @abc.abstractmethod
    def work(self):
        """Override this with your desired procedures."""
        pass


class Bootstrap(CommandMixin):

    """Update a bcbio AWS system with the latest code and tools."""

    def setup(self):
        parser = self._parser.add_parser(
            "bootstrap",
            help="Update a bcbio AWS system with the latest code and tools",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=None,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-r", "--no-reboot", default=False, action="store_true",
            help="Don't upgrade the cluster host OS and reboot")

        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        if self.args.econfig is None:
            self.args.econfig = constant.PATH.EC_CONFIG.format(
                provider=self.args.provider)

        provider = cloud_factory.get(self.args.provider)()
        response = provider.bootstrap(cluster=self.args.cluster,
                                      config=self.args.econfig,
                                      reboot=not self.args.no_reboot)
        status, report = self._process_playbook_response(response)
        if status:
            LOG.debug("All playbooks runned without problems.")
        else:
            LOG.error("Something went wrong.")
            print(report.text())


class Command(CommandMixin):

    """Run a script on the bcbio frontend node inside a screen session."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "command",
            help="Run a script on the bcbio frontend "
                 "node inside a screen session",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=None,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "script", metavar="SCRIPT",
            help="Local path of the script to run. The screen "
                 "session name is the basename of the script.")

        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(self.args.provider)()
        return provider.run_script(cluster=self.args.cluster,
                                   config=self.args.econfig,
                                   script=self.args.script)


class Setup(CommandMixin):

    """Rerun cluster configuration steps."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "setup", help="Rerun cluster configuration steps",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=None,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")

        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(self.args.provider)()
        return provider.setup(cluster=self.args.cluster,
                              config=self.args.econfig)


class Start(CommandMixin):

    """Start a bcbio cluster."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "start", help="Start a bcbio cluster",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=None,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-R", "--no-reboot", default=False, action="store_true",
            help="Don't upgrade the cluster host OS and reboot")
        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(self.args.provider)()
        status = provider.start(cluster=self.args.cluster,
                                config=self.args.econfig,
                                no_setup=False)

        if status != 0:
            LOG.error("Failed to create the cluster.")
            return

        # Run bootstrap only if the start command successfully runned.
        response = provider.bootstrap(cluster=self.args.cluster,
                                      config=self.args.econfig,
                                      reboot=not self.args.no_reboot)

        status, report = self._process_playbook_response(response)
        if status:
            LOG.debug("All playbooks runned without problems.")
        else:
            LOG.error("Something went wrong.")
            print(report.text())


class Stop(CommandMixin):

    """Stop a bcbio cluster."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "stop", help="Stop a bcbio cluster",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=None,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")

        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(self.args.provider)()
        return provider.stop(cluster=self.args.cluster,
                             config=self.args.econfig,
                             force=False,
                             use_default=False)


class SSHConnection(CommandMixin):

    """SSH to a bcbio cluster."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "ssh", help="SSH to a bcbio cluster",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=None,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "args", metavar="ARG", nargs="*",
            help="Execute the following command on the remote "
                 "machine instead of opening an interactive shell.")
        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(self.args.provider)()
        return provider.ssh(cluster=self.args.cluster,
                            config=self.args.econfig,
                            ssh_args=self.args.args)


class CreateConfig(CommandMixin):

    """Write Elasticluster configuration file with user information."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "create",
            help="Write Elasticluster configuration file.")
        parser.add_argument(
            "--econfig", help="Elasticluster bcbio configuration file",
            default=None)

        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        return common_utils.write_elasticluster_config(
            config={}, output=self.args.econfig, provider=self.args.provider)

    def task_done(self, result):
        """What to execute after successfully finished processing a task."""
        super(CreateConfig, self).task_done(result)
        LOG.info("The elasticluster config was successfully generated.")


class EditConfig(CommandMixin):

    """Edit cluster configuration."""

    def __init__(self, parent, parser):
        super(EditConfig, self).__init__(parent, parser)
        self._raw_parser = six.moves.configparser.RawConfigParser()
        self._setup_provider = "ansible"
        self._frontend_section = None
        self._cluster_section = None
        self._frontend = None
        self._cluster = None

    @staticmethod
    def _ask(message, default):
        """Get information from the user."""
        message = "{message} [{default}]: ".format(message=message,
                                                   default=default)
        value = six.moves.input(message)
        return value or default

    def _parse_section(self, section):
        """Return a directory with all the option available in
        the given section.
        """
        return dict(self._raw_parser.items(section))

    def _setup_instances(self):
        """Ask user for information regarding the instances types."""
        compute_nodes = self._ask(
            message=("Number of cluster worker nodes (0 starts a "
                     "single machine instead of a cluster)"),
            default=self._cluster["compute_nodes"])

        if int(compute_nodes) == 0:
            compute_flavor = None
            frontend_flavor = self._ask(
                message="Machine type for single frontend worker node",
                default=self._frontend["flavor"])
        else:
            frontend_flavor = self._ask(
                message="Machine type for frontend worker node",
                default=self._frontend["flavor"])
            compute_flavor = self._ask(
                message="Machine type for compute nodes",
                default=self._cluster["flavor"])
            self._setup_provider = "ansible-slurm"

        self._raw_parser.set(self._frontend_section,
                             "flavor", frontend_flavor)
        self._raw_parser.set(self._cluster_section,
                             "compute_nodes", compute_nodes)
        if compute_flavor:
            self._raw_parser.set(self._cluster_section,
                                 "flavor", compute_flavor)

    def _setup_frontnode(self):
        """Change values regarding the frontend node."""
        nfs_size = self._ask(
            message="Size of encrypted NFS mounted filesystem, in Gb",
            default=self._frontend.get("encrypted_volume_size", 200))

        # 30 IOPS/Gb, maximum 4000 IOPS http://aws.amazon.com/ebs/details/
        iops = min(int(nfs_size) * 30, 4000)
        self._raw_parser.set(self._frontend_section,
                             "encrypted_volume_size", nfs_size)
        self._raw_parser.set(self._frontend_section,
                             "encrypted_volume_type", "io1")
        self._raw_parser.set(self._frontend_section,
                             "encrypted_volume_iops", iops)

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "edit", help="Edit cluster configuration")
        parser.add_argument(
            "--econfig", help="Elasticluster bcbio configuration file",
            default=None)
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")

        parser.set_defaults(work=self.run)

    def prologue(self):
        """Executed once before the command running."""
        super(EditConfig, self).prologue()
        self._raw_parser.read([self.args.econfig])
        self._frontend_section = "cluster/{cluster}/frontend".format(
            cluster=self.args.cluster)
        self._frontend = self._parse_section(self._frontend_section)

        self._cluster_section = "cluster/{cluster}".format(
            cluster=self.args.cluster)
        self._cluster = self._parse_section(self._cluster_section)

    def work(self):
        """Setup parser using the received information."""
        self._setup_instances()
        self._setup_frontnode()
        # Update the cluster setup provider
        self._raw_parser.set(self._cluster_section, "setup_provider",
                             self._setup_provider)
        # Make a copy of the current config file
        common_utils.backup(self.args.econfig, delete=True)
        # Update the config file with the new setup
        with open(self.args.econfig, "w") as file_handle:
            self._raw_parser.write(file_handle)
