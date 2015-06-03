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

from bcbiovm.client import base
from bcbiovm.client import commands as client_commands
from bcbiovm.client.subcommands import factory
from bcbiovm.common import cluster


class BCBioParser(base.Parser):

    """bcbio-nextgen-vm command line application."""

    commands = [
        factory.get('docker', 'Run'),
        factory.get('docker', 'Install'),
        factory.get('docker', 'Upgrade'),
        factory.get('ipython', 'IPython'),
        factory.get('ipython', 'IPythonPrep'),
        client_commands.Template,
        client_commands.Provider,
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
        parser = argparse.ArgumentParser(
            description=("Automatic installation for bcbio-nextgen pipelines,"
                         " with docker."))
        parser.add_argument(
            "--datadir",
            help="Directory with genome data and associated files.",
            type=lambda x: (os.path.abspath(os.path.expanduser(x))))
        self._parser = parser.add_subparsers(title="[sub-commands]")


def main():
    """Run the bcbio-nextgen-vm command line application."""
    if len(sys.argv) > 1 and sys.argv[1] == "elasticluster":
        sys.exit(cluster.ElastiCluster.execute(sys.argv[1:]))

    bcbio = BCBioParser()
    bcbio.run()


if __name__ == "__main__":
    main()
