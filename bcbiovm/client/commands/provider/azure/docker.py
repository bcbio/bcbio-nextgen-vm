"""Utilities to help with develping using bcbion inside of docker."""

from bcbiovm import config as bcbio_config
from bcbiovm.client.commands.container import docker
from bcbiovm.provider.azure import storage
from bcbiovm.provider.azure import azure_provider


class Build(docker.Build):

    """Build docker image and export to the cloud provider."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "dockerbuild",
            help="Build docker image and export to the cloud provider.")
        parser.add_argument(
            "-t", "--buildtype", default="full", choices=["full", "code"],
            help=("Type of docker build to do. full is all code and third"
                  " party tools. code is only bcbio-nextgen code."))
        parser.add_argument(
            "-d", "--rundir", default="/tmp/bcbio-docker-build",
            help="Directory to run docker build in")
        parser.add_argument(
            "-c", "--container", default="bcbio_nextgen",
            help="The container name where to upload the gzipped "
                 "docker image to")
        parser.add_argument(
            "-s", "--storage-account",
            default=bcbio_config.get("env.STORAGE_ACCOUNT", None),
            help="The storage account name. All access to Azure Storage"
                 " is done through a storage account.")
        parser.add_argument(
            "-k", "--access-key",
            default=bcbio_config.get("env.STORAGE_ACCESS_KEY", None),
            help="The key required to access the storage account.")

        parser.set_defaults(work=self.run)

    def prologue(self):
        """Executed once before the running of the command."""
        self.args.storage = storage.AzureBlob()
        self.args.context = {
            "credentials": {
                "storage_account": self.args.storage_account,
                "storage_access_key": self.args.access_key,
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
            action="append", default=[], choices=self.SUPPORTED_GENOMES)
        parser.add_argument(
            "--aligners", help="Aligner indexes to download",
            action="append", default=[], choices=self.SUPPORTED_INDEXES)
        parser.add_argument(
            "-s", "--storage-account",
            default=bcbio_config.get("env.STORAGE_ACCOUNT", None),
            help="The storage account name. All access to Azure Storage"
                 " is done through a storage account.")
        parser.add_argument(
            "-k", "--access-key",
            default=bcbio_config.get("env.STORAGE_ACCESS_KEY", None),
            help="The key required to access the storage account.")

        parser.set_defaults(work=self.run)

    def prologue(self):
        """Executed once before the arguments parsing."""
        super(BiodataUpload, self).prologue()
        self.args.provider = azure_provider.AzureProvider()
        self.args.context = {
            "credentials": {
                "storage_account": self.args.storage_account,
                "storage_access_key": self.args.access_key
            }
        }
