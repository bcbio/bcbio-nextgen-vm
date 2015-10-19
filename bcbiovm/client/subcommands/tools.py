"""Tools and utilities commands."""

from bcbiovm.client import base
from bcbiovm.provider.aws import storage as aws_storage
from bcbiovm.provider.azure import storage as azure_storage


class _S3Upload(base.Command):

    """Upload file to Amazon Simple Storage Service (Amazon S3)."""

    def __init__(self, parent, parser):
        super(_S3Upload, self).__init__(parent, parser)
        self._storage = aws_storage.AmazonS3()

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
        return self._storage.upload(path=self.args.file,
                                    filename=self.args.key,
                                    container=self.args.bucket)


class _BlobUpload(base.Command):

    """Upload file to Azure Blob storage service."""

    def __init__(self, parent, parser):
        super(_BlobUpload, self).__init__(parent, parser)
        self._storage = azure_storage.AzureBlob

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
        context = {"account_name": self.args.account_name}
        return self._storage.upload(path=self.args.file,
                                    container=self.args.container,
                                    filename=self.args.blob,
                                    context=context)


class Upload(base.Container):

    """Upload file to a storage manager."""

    sub_commands = [
        (_S3Upload, "storage_manager"),
        (_BlobUpload, "storage_manager"),
    ]

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "upload", help="Upload file to a storage manager.")

        storage_manager = parser.add_subparsers(title="[storage manager]")
        self._register_parser("storage_manager", storage_manager)
