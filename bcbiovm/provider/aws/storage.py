"""Manage pushing and pulling files from Amazon Web Services S3."""

import os

import boto
import yaml
from bcbio import utils as bcbio_utils
from bcbio.distributed import objectstore

from bcbiovm.common import utils as common_utils
from bcbiovm.provider import storage


class AmazonS3(storage.StorageManager, objectstore.AmazonS3):

    """Amazon Simple Storage Service (Amazon S3) Manager."""

    _UPLOAD_HEADERS = {
        "x-amz-storage-class": "REDUCED_REDUNDANCY",
        "x-amz-server-side-encryption": "AES256",
    }

    @classmethod
    def get_bucket(cls, bucket_name):
        """Retrieves a bucket by name."""
        connection = boto.connect_s3()
        try:
            # If the bucket does not exist, an S3ResponseError
            # will be raised.
            bucket = connection.get_bucket(bucket_name)
        except boto.exception.S3ResponseError as exc:
            if exc.status == 404:
                bucket = connection.create_bucket(bucket_name)
            else:
                raise

        return bucket

    def exists(self, container, filename, context=None):
        """Check if the received key name exists in the bucket.

        :container: The name of the bucket.
        :filename:  The name of the key.
        :context:   More information required by the storage manager.
        """
        super(AmazonS3, self).exists(container, filename, context)
        bucket = self.get_bucket(container)
        key = bucket.get_key(filename)
        return True if key else False

    @classmethod
    def upload(cls, path, filename, container, context=None):
        """Upload the received file.

        :path:      The path of the file that should be uploaded.
        :container: The name of the bucket.
        :filename:  The name of the key.
        :context:   More information required by the storage manager.
        """
        headers = (context or {}).get("headers", cls._UPLOAD_HEADERS)
        arguments = (context or {}).get("arguments", [])

        command = ["gof3r", "put", "-p", path,
                   "-k", filename, "-b", container]
        command.extend(arguments)

        if headers:
            for header, value in headers.items():
                command.extend(("-m", "{0}:{1}".format(header, value)))

        common_utils.execute(command, check_exit_code=True)

    @classmethod
    def load_config(cls, sample_config):
        """Move a sample configuration locally, providing remote upload."""
        with cls.open(sample_config) as s3_handle:
            config = yaml.load(s3_handle)

        # The file_info is a namedtuple which contains the following fields:
        # ["store", "bucket", "key", "region"]
        file_info = cls.parse_remote(sample_config)
        config["upload"] = {
            "method": "s3",
            "dir": os.path.join(os.pardir, "final"),
            "container": file_info.bucket,
            "folder": os.path.join(os.path.dirname(file_info.key), "final"),
            "region": file_info.region or cls.get_region(),
        }

        out_file = os.path.join(
            bcbio_utils.safe_makedir(os.path.join(os.getcwd(), "config")),
            os.path.basename(file_info.key))
        cls._export_config(list_function=cls.list, config=config,
                           sample_config=sample_config, out_file=out_file)

        return out_file
