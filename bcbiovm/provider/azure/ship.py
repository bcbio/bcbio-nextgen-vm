"""Prepare a running process to execute remotely and reconstitute
an analysis in a temporary directory on the current machine.
"""
import os
import re

import azure
import toolz
from azure import storage
from bcbio import utils
from bcbio.distributed import objectstore
from bcbio.pipeline import config_utils


from bcbiovm.docker import remap
from bcbiovm.provider import base

BLOB_NAME = "{folder}/{filename}"
BLOB_FILE = ("https://{storage}.blob.core.windows.net/"
             "{container}/{blob}")


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
            blob_handle._download_chunk(chunk_offset=0)
        except azure.WindowsAzureMissingResourceError:
            return False

        return True

    @classmethod
    def connect(cls, account_name, account_key=None):
        """Returns a connection object pointing to the endpoint
        associated to the received blob service.
        """
        if account_key is None:
            # FIXME(alexandrucoman): Define the env variable name
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


class BlobPack(base.Pack):

    """Prepare a running process to execute remotely, moving files
    as necessary to shared infrastructure.
    """

    def __init__(self):
        super(BlobPack, self).__ini__()

    @staticmethod
    def _upload(filename, account_name, container, blob_name):
        """Upload the received file.

        :filename:     The file path for the file which will be uploaded.
        :account_name: The storage account name. All access to Azure Storage
                       is done through a storage account.
        :container:    The name of the container that contains the blob. All
                       blobs must be in a container.
        :blob_name:    The name of the blob.
        """
        return AzureBlob.upload(filename, account_name, container, blob_name)

    def _upload_if_not_exists(self, filename, store):
        """Upload the received file if not exists."""
        account_name = store["storage_account"]
        container = store["container"]
        blob_name = BLOB_NAME.format(folder=store["folder"],
                                     filename=os.path.basename(filename))

        if not AzureBlob.exists(account_name, container, blob_name):
            self._upload(filename, account_name, container, blob_name)

    def _remap_and_ship(self, orig_fname, context, remap_dict):
        """Remap a file into an Azure blob and key, shipping if not present.

        Uploads files if not present in the specified blob.
        """
        # pylint: disable=unused-argument
        if not os.path.isfile(orig_fname):
            return None

        dirname = os.path.dirname(os.path.abspath(orig_fname))
        store = remap_dict[os.path.normpath(dirname)]
        for filename in utils.file_plus_index(orig_fname):
            self._upload_if_not_exists(filename, store)

        blob = BLOB_NAME.format(folder=store["folder"],
                                filename=os.path.basename(orig_fname))
        return BLOB_FILE.format(storage=store["storage"],
                                blob=blob, container=store["container"])

    def send_output(self, config, out_file):
        """Send an output file with state information from a run."""
        account_name = toolz.get_in(["storage_account"], config)
        container = toolz.get_in(["containers", "run"], config)
        blob_name = BLOB_NAME.format(
            folder=toolz.get_in(["folders", "output"], config),
            filename=os.path.basename(out_file))
        self._upload(out_file, account_name, container, blob_name)


class ReconstituteBlob(base.Reconstitute):

    """Reconstitute an analysis in a temporary directory on the
    current machine.

    Handles copying or linking files into a work directory,
    running an analysis, then handing off outputs to ship
    back to subsequent processing steps.
    """

    _URL_FORMAT = re.compile(r'http.*\/\/(?P<storage>[^.]+)[^/]+\/'
                             r'(?P<container>[^/]+)\/*(?P<blob>.*)')

    @staticmethod
    def _download(source, destination):
        """Download file from Azure Blob Storage Service."""
        if os.path.exists(destination):
            return

        download_directory = os.path.dirname(destination)
        utils.safe_makedir(download_directory)
        AzureBlob.download(filename=source, input_dir=None,
                           dl_dir=download_directory)

    def _unpack(self, account_name, container, args):
        """Create local directory in current directory with pulldowns
        from the Azure Blob Service.
        """
        local_dir = utils.safe_makedir(os.path.join(os.getcwd(), container))
        suffix = 'https://{storage}/{container}'.format(storage=account_name,
                                                        container=container)
        regexp = re.compile(suffix)

        def _callback(orig_fname, context, remap_dict):
            """Pull down s3 published data locally for processing."""
            # pylint: disable=unused-argument

            if not regexp.match(orig_fname):
                return orig_fname

            if context[0] in ["reference", "genome_resources", "sam_ref"]:
                cur_dir = os.path.join(local_dir, "genomes")
            else:
                cur_dir = local_dir

            for fname in utils.file_plus_index(orig_fname):
                out_fname = regexp.sub(cur_dir, fname)
                self._download(source=fname, destination=out_fname)

            return regexp.sub(cur_dir, orig_fname)

        new_args = remap.walk_files(args, _callback, {suffix: local_dir})
        return (local_dir, new_args)

    def prepare_workdir(self, pack, parallel, args):
        """Unpack necessary files and directories into a temporary structure
        for processing.
        """
        blob_pack = BlobPack()
        workdir, new_args = self._unpack(account_name=pack["storage_account"],
                                         container=pack["containers"]["run"],
                                         args=args)
        datai, data = config_utils.get_dataarg(new_args)
        if "dirs" not in data:
            data["dirs"] = {}
        data["dirs"]["work"] = workdir
        new_args[datai] = data
        return (workdir, new_args, blob_pack.send_run_integrated(pack))

    def get_output(self, target_file, pconfig):
        """Retrieve an output file from pack configuration."""
        blob_name = BLOB_NAME.format(
            folder=toolz.get_in(["folders", "output"], pconfig),
            filename=os.path.basename(target_file))
        blob_url = BLOB_FILE.format(
            storage=toolz.get_in(["storage_account"], pconfig),
            container=toolz.get_in(["containers", "run"], pconfig),
            blob=blob_name)

        self._download(source=blob_url, destination=target_file)
        return target_file

    def prepare_datadir(self, pack, args):
        """Prepare the biodata directory."""
        if pack["type"] == "blob":
            return self._unpack(account_name=pack["storage_account"],
                                container=pack["containers"]["run"],
                                args=args)

        return super(ReconstituteBlob, self).prepare_datadir(pack, args)
