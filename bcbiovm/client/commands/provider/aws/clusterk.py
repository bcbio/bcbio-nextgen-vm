import os

from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.provider.aws.clusterk import main as clusterk_main


class ClusterK(base.Command):

    """Run on Amazon web services using Clusterk."""

    def __init__(self, parent, parser):
        super(ClusterK, self).__init__(parent, parser)

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "clusterk",
            help="Run on Amazon web services using Clusterk.")
        parser.add_argument(
            "sample_config",
            help="YAML file with details about samples to process.")
        parser.add_argument(
            "--fcdir",
            help="A directory of Illumina output or fastq files to process",
            type=lambda path: (os.path.abspath(os.path.expanduser(path))))
        parser.add_argument(
            "--systemconfig",
            help=("Global YAML configuration file specifying system details. "
                  "Defaults to installed bcbio_system.yaml."))
        parser.add_argument(
            "-n", "--numcores", type=int, default=1,
            help="Total cores to use for processing")
        parser.add_argument(
            "run_bucket",
            help="Name of the S3 bucket to use for storing run information")
        parser.add_argument(
            "biodata_bucket",
            help=("Name of the S3 bucket to use for "
                  "storing biodata like genomes"))
        parser.add_argument(
            "-q", "--queue", default="default",
            help="Clusterk queue to run jobs on.")
        parser.set_defaults(work=self.run)

    def prologue(self):
        """Executed once before the arguments parsing."""
        # Add user configured defaults to supplied command line arguments.
        self.defaults.add_defaults()
        # Retrieve supported remote inputs specified on the command line.
        self.defaults.retrieve()
        # Check if the datadir exists if it is required.
        self.defaults.check_datadir("Could not run Clusterk parallel "
                                    "analysis.")

    def work(self):
        """Run the command with the received information."""
        clusterk_main.run(self.args, constant.DOCKER)
