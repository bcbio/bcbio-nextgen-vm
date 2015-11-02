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
from bcbiovm import log as logging
from bcbiovm.client import base
from bcbiovm.client import commands as client_commands
from bcbiovm.client import groups
from bcbiovm.client.commands import container

LOG = logging.get_logger(__name__)


class BCBioClient(base.Client):

    """bcbio-nextgen-vm command line application."""

    commands = [
        (container.docker.Run, "commands"),
        (container.docker.Install, "commands"),
        (container.docker.Upgrade, "commands"),
        (container.docker.RunFunction, "commands"),
        (container.docker.SaveConfig, "commands"),
        (client_commands.common.Template, "commands"),
        (client_commands.ipython.IPython, "commands"),
        (client_commands.ipython.IPythonPrep, "commands"),
        (groups.AWSProvider, "commands"),
        (groups.AzureProvider, "commands"),
    ]

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
        commands = self._parser.add_subparsers(
            title="[commands]", dest="provider")

        self._register_parser("commands", commands)

    def prologue(self):
        """Executed once before the command running."""
        super(BCBioClient, self).prologue()

        if self.args.quiet:
            # Print only the errors and exceptions
            config["log.cli.level"] = 40
            config["log.verbosity"] = 0

        elif self.args.verbosity:
            cli_level = config["log.cli.level"] - 10 * self.args.verbosity
            config["log.cli.level"] = cli_level if cli_level > 0 else 0
            config["log.verbosity"] = self.args.verbosity

        logging.update_loggers()


def main():
    """Run the bcbio-nextgen-vm command line application."""
    if len(sys.argv) > 1 and sys.argv[1] == "elasticluster":
        sys.exit(cluster.ElastiCluster.execute(sys.argv[1:]))

    bcbio = BCBioClient(sys.argv[1:])
    bcbio.run()


if __name__ == "__main__":
    main()
