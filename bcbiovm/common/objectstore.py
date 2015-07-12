"""
Manage pushing and pulling files from an object store like
Amazon Web Services S3 or Azure Blob Service.
"""
import os

import azure
import boto
from azure import storage
from bcbio.distributed import objectstore

from bcbiovm.common import utils as common_utils


class AmazonS3(objectstore.AzureBlob):

    """Amazon Simple Storage Service (Amazon S3) Manager."""

    def __init__(self):
        super(AmazonS3, self).__init__()
        self._conn = boto.connect_s3()

    def get_bucket(self, bucket_name):
        """Retrieves a bucket by name."""
        try:
            # If the bucket does not exist, an S3ResponseError
            # will be raised.
            bucket = self._conn.get_bucket(bucket_name)
        except boto.exception.S3ResponseError as exc:
            if exc.status == 404:
                bucket = self._conn.create_bucket(bucket_name)
            else:
                raise
        return bucket

    def exists(self, bucket_name, keyname):
        bucket = self.get_bucket(bucket_name)
        key = bucket.get_key(keyname)
        return True if key else False

    @classmethod
    def upload(cls, filename, key, container):
        """Upload the received file."""
        common_utils.execute(
            ["gof3r", "put", "-p", filename,
             "-k", key, "-b", container,
             "-m", "x-amz-storage-class:REDUCED_REDUNDANCY",
             "-m", "x-amz-server-side-encryption:AES256"],
            check_exit_code=True)


class AzureBlob(objectstore.AzureBlob):

    """Azure Blob storage service manager."""

    def __init__(self):
        super(AzureBlob, self).__init__()

    @staticmethod
    def exists(account_name, container, blob_name):
        """Check if the blob exists.

        :account_name: The storage account name. All access to Azure Storage
                       is done through a storage account.
        :container:    The name of the container that contains the blob. All
                       blobs must be in a container.
        :blob_name:    The name of the blob.
        """
        blob_handle = objectstore.BlobHandle(
            blob_service=account_name, container=container,
            blob=blob_name, chunk_size=32)
        try:
            # pylint: disable=protected-access
            blob_handle._download_chunk(chunk_offset=0, chunk_size=1024)
        except azure.WindowsAzureMissingResourceError:
            return False

        return True

    @classmethod
    def connect(cls, account_name):
        """Returns a connection object pointing to the endpoint
        associated to the received blob service.
        """
        account_key = os.getenv("BLOB_ACCOUNT_KEY", None)
        return storage.BlobService(account_name=account_name,
                                   account_key=account_key)

    @classmethod
    def upload(cls, filename, account_name, container, blob_name):
        """Upload the received file.

        :filename:     The file path for the file which will be uploaded.
        :account_name: The storage account name. All access to Azure Storage
                       is done through a storage account.
        :container:    The name of the container that contains the blob. All
                       blobs must be in a container.
        :blob_name:    The name of the blob.
        """
        blob_service = cls.connect(account_name)
        blob_service.put_block_blob_from_path(container_name=container,
                                              blob_name=blob_name,
                                              file_path=filename)
