"""Manage pushing and pulling files from Azure Blob Service."""

import os

import azure
import yaml
from azure import storage as azure_storage
from bcbio import utils as bcbio_utils
from bcbio.distributed import objectstore

from bcbiovm import config as bcbiovm_config
from bcbiovm.common import exception
from bcbiovm.provider import storage


class AzureBlob(storage.StorageManager, objectstore.AzureBlob):

    """Azure Blob storage service manager."""

    @classmethod
    def connect(cls, resource=None):
        """Returns a connection object pointing to the endpoint
        associated to the received blob service.
        """
        if resource:
            return objectstore.AzureBlob.connect(resource)

        account_name = bcbiovm_config.get("env.STORAGE_ACCOUNT", None)
        if not account_name:
            raise exception.NotFound(object="account_name",
                                     container=bcbiovm_config.env)

        return azure_storage.BlobService(
            account_name=account_name,
            account_key=bcbiovm_config.get("env.STORAGE_ACCESS_KEY", None))

    @classmethod
    def exists(cls, container, filename, context=None):
        """Check if the received key name exists in the bucket.

        :container: The name of the container that contains the blob. All
                    blobs must be in a container.
        :filename:  The name of the blob.
        :context:   More information required by the storage manager.

        :notes:
            The context should contain the storage account name.
            All access to Azure Storage is done through a storage account.
        """
        account_name = (context or {}).get('account_name', None)
        if not account_name:
            raise exception.NotFound(object="account_name",
                                     container="context: {0}".format(context))

        blob_handle = objectstore.BlobHandle(blob_service=account_name,
                                             container=container,
                                             blob=filename, chunk_size=32)
        try:
            # pylint: disable=protected-access
            blob_handle._download_chunk(chunk_offset=0, chunk_size=1024)
        except azure.WindowsAzureMissingResourceError:
            return False

        return True

    @classmethod
    def upload(cls, path, filename, container, context=None):
        """Upload the received file.

        :path:       The path of the file that should be uploaded.
        :container:  The name of the container that contains the blob. All
                     blobs must be in a container.
        :filename:   The name of the blob.
        :context:    More information required by the storage manager.

        :notes:
            The context should contain the storage account name.
            All access to Azure Storage is done through a storage account.
        """
        blob_service = cls.connect()
        blob_service.put_block_blob_from_path(container_name=container,
                                              blob_name=filename,
                                              file_path=path)

    @classmethod
    def load_config(cls, sample_config):
        """Move a sample configuration locally, providing remote upload."""
        with cls.open(sample_config) as blob_handle:
            config = yaml.load(blob_handle)

        # The file_info is a namedtuple which contains the following fields:
        # ["store", "storage", "container", "blob"]
        file_info = cls.parse_remote(sample_config)
        config["upload"] = {
            "method": "blob",
            "dir": os.path.join(os.pardir, "final"),
            "container": file_info.container,
            "folder": os.path.join(os.path.dirname(file_info.blob), "final"),
            "storage_account": file_info.storage,
        }

        out_file = os.path.join(
            bcbio_utils.safe_makedir(os.path.join(os.getcwd(), "config")),
            os.path.basename(file_info.blob))
        cls._export_config(list_function=cls.list, config=config,
                           sample_config=sample_config, out_file=out_file)

        return out_file
