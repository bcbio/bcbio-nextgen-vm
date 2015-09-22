"""Tools and utilities commands."""

from bcbiovm.client import base
from bcbiovm.common import objectstore


class _S3Upload(base.Command):

    """Upload file to Amazon Simple Storage Service (Amazon S3)."""

    def __init__(self, *args, **kwargs):
        super(_S3Upload, self).__init__(*args, **kwargs)
        self._storage = objectstore.AmazonS3()

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "aws", help="Amazon Simple Storage Service (Amazon S3)")
        parser.add_argument(
            "--file", required=True,
            help="The file path for the file which will be uploaded.")
        parser.add_argument(
            "--key", required=True,
            help="The name of the file.")
        parser.add_argument(
            "--bucket", required=True,
            help="The name of the container that contains the file.")

        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        return self._storage.upload(filename=self.args.file,
                                    key=self.args.key,
                                    container=self.args.bucket)


class _BlobUpload(base.Command):

    """Upload file to Azure Blob storage service."""

    def __init__(self, *args, **kwargs):
        super(_BlobUpload, self).__init__(*args, **kwargs)
        self._storage = objectstore.AzureBlob

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "azure", help="Azure Blob storage service")
        parser.add_argument(
            "--file", required=True,
            help="The file path for the file which will be uploaded.")
        parser.add_argument(
            "--blob", required=True,
            help="The name of the blob.")
        parser.add_argument(
            "--container", required=True,
            help="The name of the container that contains the blob. All "
                 "blobs must be in a container.")
        parser.add_argument(
            "--account_name", default=None,
            help="The storage account name. All access to Azure Storage"
                 " is done through a storage account.")

        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        return self._storage.upload(filename=self.args.file,
                                    account_name=self.args.account_name,
                                    container=self.args.container,
                                    blob_name=self.args.blob)


class Upload(base.Command):

    """Upload file to the file storage provider."""

    sub_commands = [
        (_S3Upload, "storage_manager"),
        (_BlobUpload, "storage_manager"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "upload",
            help=("Utilities to help with develping using bcbion"
                  "inside of docker."))
        storage_manager = parser.add_subparsers(title="[storage manager]")
        self._register_parser("storage_manager", storage_manager)

    def work(self):
        """Run the command with the received information."""
        pass
