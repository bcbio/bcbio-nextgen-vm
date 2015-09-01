#!/usr/bin/env python -E
"""Run and install bcbio-nextgen, using code and tools isolated
in a docker container.

See the bcbio documentation https://bcbio-nextgen.readthedocs.org
for more details about running it for analysis.
"""
from __future__ import print_function
import argparse
import os
import sys

from bcbiovm import config
from bcbiovm.client import base
from bcbiovm.client import commands as client_commands
from bcbiovm.client.subcommands import factory
from bcbiovm.common import cluster


class BCBioParser(base.BaseParser):

    """bcbio-nextgen-vm command line application."""

    commands = [
        factory.get('docker', 'Run'),
        factory.get('docker', 'Install'),
        factory.get('docker', 'Upgrade'),
        factory.get('ipython', 'IPython'),
        factory.get('ipython', 'IPythonPrep'),
        client_commands.Template,
        client_commands.AWSProvider,
        client_commands.AzureProvider,
        # TODO(alexandrucoman): Add elasticluster command
        factory.get('docker', 'RunFunction'),
        client_commands.DockerDevel,
        factory.get('docker', 'SaveConfig')
    ]

    def check_command(self, command):
        """Check if the received command is valid and can be used property."""
        return isinstance(command, base.BaseCommand)

    def setup(self):
        """Extend the parser configuration in order to expose all
        the received commands.
        """
        self._parser = argparse.ArgumentParser(
            description=("Automatic installation for bcbio-nextgen pipelines,"
                         " with docker."))
        self._parser.add_argument(
            "--datadir",
            help="Directory with genome data and associated files.",
            type=lambda x: (os.path.abspath(os.path.expanduser(x))))
        self._parser.add_argument(
            "-q", "--quiet", dest="quiet", action="store_true",
            default=False, help="Quiet output when running Ansible playbooks")
        self._parser.add_argument(
            "-v", "--verbosity", dest="verbosity", action="count",
            help="increase output verbosity")

        self._subparser = self._parser.add_subparsers(
            title="[sub-commands]", dest="provider")

    def epilogue(self):
        """Executed once before the command running."""
        if self.args.quiet:
            # Print only the errors and exceptions
            config.log["cli_level"] = 40
            config.log["enabled"] = False
        elif self.args.verbosity:
            cli_level = config.log["cli_level"] - 10 * self.args.verbosity
            config.log["cli_level"] = cli_level if cli_level > 0 else 0
            config.log["verbosity"] = self.args.verbosity


def main():
    """Run the bcbio-nextgen-vm command line application."""
    if len(sys.argv) > 1 and sys.argv[1] == "elasticluster":
        sys.exit(cluster.ElastiCluster.execute(sys.argv[1:]))

    bcbio = BCBioParser(sys.argv[1:])
    bcbio.run()


if __name__ == "__main__":
    main()
