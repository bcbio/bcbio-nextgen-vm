"""Run and manage a cluster using elasticluster."""

import argparse

from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.provider import factory as cloud_factory


class Bootstrap(base.BaseCommand):

    """Update a bcbio AWS system with the latest code and tools."""

    def setup(self):
        parser = self._main_parser.add_parser(
            "bootstrap",
            help="Update a bcbio AWS system with the latest code and tools",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-R", "--no-reboot", default=False, action="store_true",
            help="Don't upgrade the cluster host OS and reboot")
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(self.args.provider)
        return provider.bootstrap(cluster=self.args.cluster,
                                  config=self.args.econfig,
                                  reboot=not self.args.no_reboot,
                                  verbose=self.args.verbose)


class Command(base.BaseCommand):

    """Run a script on the bcbio frontend node inside a screen session."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "command",
            help="Run a script on the bcbio frontend "
                 "node inside a screen session",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "script", metavar="SCRIPT",
            help="Local path of the script to run. The screen "
                 "session name is the basename of the script.")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(self.args.provider)
        return provider.run_script(cluster=self.args.cluster,
                                   config=self.args.econfig,
                                   script=self.args.script)


class Setup(base.BaseCommand):

    """Rerun cluster configuration steps."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "setup", help="Rerun cluster configuration steps",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(self.args.provider)
        return provider.setup(cluster=self.args.cluster,
                              config=self.args.econfig,
                              verbose=self.args.verbose)


class Start(base.BaseCommand):

    """Start a bcbio cluster."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "start", help="Start a bcbio cluster",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-R", "--no-reboot", default=False, action="store_true",
            help="Don't upgrade the cluster host OS and reboot")
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(self.args.provider)
        status = provider.start(cluster=self.args.cluster,
                                config=self.args.econfig,
                                no_setup=False,
                                verbose=self.args.verbose)

        if status == 0:
            # Run bootstrap only if the start command successfully runned.
            status = provider.bootstrap(cluster=self.args.cluster,
                                        config=self.args.econfig,
                                        reboot=not self.args.no_reboot,
                                        verbose=self.args.verbose)
        return status


class Stop(base.BaseCommand):

    """Stop a bcbio cluster."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "stop", help="Stop a bcbio cluster",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(self.args.provider)
        return provider.stop(cluster=self.args.cluster,
                             config=self.args.econfig,
                             force=False,
                             use_default=False,
                             verbose=self.args.verbose)


class SSHConnection(base.BaseCommand):

    """SSH to a bcbio cluster."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "ssh", help="SSH to a bcbio cluster",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "--econfig", default=constant.PATH.EC_CONFIG,
            help="Elasticluster bcbio configuration file")
        parser.add_argument(
            "-c", "--cluster", default="bcbio",
            help="elasticluster cluster name")
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.add_argument(
            "args", metavar="ARG", nargs="*",
            help="Execute the following command on the remote "
                 "machine instead of opening an interactive shell.")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        provider = cloud_factory.get(self.args.provider)
        return provider.ssh(cluster=self.args.cluster,
                            config=self.args.econfig,
                            ssh_args=self.args.args,
                            verbose=self.args.verbose)
