"""
Manage pushing and pulling files from an object store like
Amazon Web Services S3 or Azure Blob Service.
"""
import abc
import os

import azure
import boto
import six
import yaml

from azure import storage
from bcbio import utils as bcbio_utils
from bcbio.distributed import objectstore

from bcbiovm.common import utils as common_utils
from bcbiovm.common import exception


@six.add_metaclass(abc.ABCMeta)
class StorageManager(object):

    """The contract class for all the storage managers."""

    _ACCESS_ERROR = (
        "Cannot write to the parent directory of work directory %(cur_dir)s\n"
        "bcbio wants to store prepared uploaded files to %(final_dir)s\n"
        "We recommend structuring your project in a project specific "
        "directory structure\n"
        "with a specific work directory (mkdir -p your-project/work "
        "&& cd your-project/work)."
    )
    _JAR_RESOURCES = {
        "genomeanalysistk": "gatk",
        "mutect": "mutect"
    }

    @classmethod
    def _jar_resources(cls, list_function, sample_config):
        """Find uploaded jars for GATK and MuTect relative to input file.

        Automatically puts these into the configuration file to make them
        available for downstream processing. Searches for them in the specific
        project folder and also a global jar directory for a container.
        """
        configuration = {}
        jar_directory = os.path.join(os.path.dirname(sample_config), "jars")

        for filename in list_function(jar_directory):
            program = None
            for marker in cls._JAR_RESOURCES:
                if marker in filename.lower():
                    program = cls._JAR_RESOURCES[marker]
                    break
            else:
                continue

            resources = configuration.setdefault("resources", {})
            program = resources.setdefault(program, {})
            program["jar"] = filename

        return configuration

    @classmethod
    def _export_config(cls, list_function, config, sample_config, out_file):
        """Move a sample configuration locally."""
        if not os.access(os.pardir, os.W_OK | os.X_OK):
            raise IOError(cls._ACCESS_ERROR % {
                "final_dir": os.path.join(os.pardir, "final"),
                "cur_dir": os.getcwd()})

        config.update(cls._jar_resources(list_function, sample_config))
        with open(out_file, "w") as out_handle:
            yaml.dump(config, out_handle, default_flow_style=False,
                      allow_unicode=False)

    @abc.abstractmethod
    def exists(self, container, filename, context=None):
        """Check if the received key name exists in the bucket.

        :container: The name of the container.
        :filename:  The name of the item from the container.
        :context:   More information required by the storage manager.
        """
        pass

    @abc.abstractmethod
    def upload(self, path, filename, container, context=None):
        """Upload the received file.

        :path:      The path of the file that should be uploaded.
        :container: The name of the container.
        :filename:  The name of the item from the container.
        :context:   More information required by the storage manager.
        """
        pass

    @abc.abstractmethod
    def load_config(self, sample_config):
        """Move a sample configuration locally, providing remote upload."""
        pass


class AmazonS3(StorageManager, objectstore.AmazonS3):

    """Amazon Simple Storage Service (Amazon S3) Manager."""

    _UPLOAD_HEADERS = {
        "x-amz-storage-class": "REDUCED_REDUNDANCY",
        "x-amz-server-side-encryption": "AES256",
    }

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
        command = ["gof3r", "put", "-p", path,
                   "-k", filename, "-b", container]
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


class AzureBlob(StorageManager, objectstore.AzureBlob):

    """Azure Blob storage service manager."""

    def __init__(self):
        super(AzureBlob, self).__init__()

    @classmethod
    def connect(cls, account_name):
        """Returns a connection object pointing to the endpoint
        associated to the received blob service.
        """
        account_key = os.getenv("BLOB_ACCOUNT_KEY", None)
        return storage.BlobService(account_name=account_name,
                                   account_key=account_key)

    @staticmethod
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
        account_name = (context or {}).get('account_name', None)
        if not account_name:
            raise exception.NotFound(object="account_name",
                                     container="context: {0}".format(context))
        blob_service = cls.connect(account_name)
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
