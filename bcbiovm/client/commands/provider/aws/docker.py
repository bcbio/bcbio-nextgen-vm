"""Utilities to help with develping using bcbion inside of docker."""

from bcbiovm.client.commands.container import docker
from bcbiovm.provider.aws import storage
from bcbiovm.provider.aws import aws_provider


class Build(docker.Build):

    """Build docker image and export to the cloud provider."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "dockerbuild",
            help="Build docker image and export to the cloud provider.")
        parser.add_argument(
            "-c", "--container", default="bcbio_nextgen",
            help="The container name where to upload the gzipped "
                 "docker image to")
        parser.add_argument(
            "-t", "--buildtype", default="full", choices=["full", "code"],
            help=("Type of docker build to do. full is all code and third"
                  " party tools. code is only bcbio-nextgen code."))
        parser.add_argument(
            "-d", "--rundir", default="/tmp/bcbio-docker-build",
            help="Directory to run docker build in")

        parser.set_defaults(work=self.run)

    def prologue(self):
        """Executed once before the running of the command."""
        self.args.storage = storage.AmazonS3()
        self.args.context = {
            "headers": {
                "x-amz-storage-class": "REDUCED_REDUNDANCY",
                "x-amz-acl": "public-read",
            }
        }


class BiodataUpload(docker.BiodataUpload):

    """Upload pre-prepared biological data to cache."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "biodata",
            help="Upload pre-prepared biological data to cache")
        parser.add_argument(
            "--prepped",
            help=("Start with an existing set of cached data to "
                  "output directory."))
        parser.add_argument(
            "--genomes", help="Genomes to download",
            action="append", default=[],
            choices=["GRCh37", "hg19", "mm10", "mm9", "rn5", "canFam3", "dm3",
                     "Zv9", "phix", "sacCer3", "xenTro3", "TAIR10",
                     "WBcel235"])
        parser.add_argument(
            "--aligners", help="Aligner indexes to download",
            action="append", default=[],
            choices=["bowtie", "bowtie2", "bwa", "novoalign", "star", "ucsc"])

        parser.set_defaults(work=self.run)

    def prologue(self):
        """Executed once before the arguments parsing."""
        super(BiodataUpload, self).prologue()
        self.args.provider = aws_provider.AWSProvider()
        self.args.context = {
            "arguments": ["--no-md5"],
            "headers": {
                "x-amz-storage-class": "REDUCED_REDUNDANCY",
                "x-amz-acl": "public-read"
            }
        }
