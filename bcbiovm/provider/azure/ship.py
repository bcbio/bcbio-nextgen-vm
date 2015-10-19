"""Prepare a running process to execute remotely and reconstitute
an analysis in a temporary directory on the current machine.
"""
import os
import re

from bcbio import utils
from bcbio.pipeline import config_utils

from bcbiovm.common import objects
from bcbiovm.container.docker import remap as docker_remap
from bcbiovm.provider import base
from bcbiovm.provider.azure import storage as azure_storage

BLOB_NAME = "{folder}/{filename}"
BLOB_FILE = ("https://{storage}.blob.core.windows.net/"
             "{container}/{blob}")


def get_shipping_config(biodata_container, run_container, output_folder,
                        storage_account):
    """Prepare configuration for shipping to Azure Blob Service."""
    config = {
        "type": "blob",
        "storage_account": storage_account,
        "blobs": {
            "run": run_container,
            "biodata": biodata_container,
        },
        "folders": {
            "output": output_folder
        }
    }
    return config


def shipping_config(config):
    """Create a ShippingConfig object with the received information."""
    blob_config = objects.ShippingConfig(config)
    blob_config.add_alias(container="blobs", alias="containers")
    return blob_config


class BlobPack(base.Pack):

    """Prepare a running process to execute remotely, moving files
    as necessary to shared infrastructure.
    """

    def __init__(self):
        super(BlobPack, self).__ini__()
        self._storage = azure_storage.AzureBlob()

    def _upload_if_not_exists(self, account, container, folder, path):
        """Upload the received file if not exists.

        :account:       The storage account name. All access to Azure Storage
                        is done through a storage account.
        :container:     The name of the container that contains the blob. All
                        blobs must be in a container.
        :param folder:  The name of the folder where the file will be stored.
        :param path:    The name of the container from the storage service.
        """
        context = {"account_name": account}
        blob_name = BLOB_NAME.format(folder=folder,
                                     filename=os.path.basename(path))

        if not self._storage.exists(container, blob_name, context):
            self._storage.upload(path=path, container=container,
                                 filename=blob_name, context=context)

    def _remap_and_ship(self, orig_fname, context, remap_dict):
        """Remap a file into an Azure blob and key, shipping if not present.

        Uploads files if not present in the specified blob.

        Each value from :param remap_dict: is an directory wich contains
        the following keys:
            * container:        The name of the container that contains
                                the blob. All blobs must be in a container.
            * folder            The name of the folder where the file
                                will be stored.
            * shipping_config   an instance of :class objects.ShippingConfig:
        """
        # pylint: disable=unused-argument
        if not os.path.isfile(orig_fname):
            return None

        dirname = os.path.dirname(os.path.abspath(orig_fname))
        store = remap_dict[os.path.normpath(dirname)]
        config = store["shipping_config"]

        for file_path in utils.file_plus_index(orig_fname):
            self._upload_if_not_exists(account=config.storage_account,
                                       container=store["container"],
                                       folder=store["folder"],
                                       path=file_path)

        blob = BLOB_NAME.format(folder=store["folder"],
                                filename=os.path.basename(orig_fname))
        return BLOB_FILE.format(storage=config.storage_account,
                                blob=blob, container=store["container"])

    def send_output(self, config, out_file):
        """Send an output file with state information from a run.

        :param config: an instances of :class objects.ShippingConf:
        """
        context = {"account_name": config.storage_account}
        blob_name = BLOB_NAME.format(folder=config.folders["output"],
                                     filename=os.path.basename(out_file))
        self._storage.upload(path=out_file, filename=blob_name,
                             container=config.container["run"],
                             context=context)

    def send_run(self, args, config):
        """Ship required processing files to the storage service for running
        on non-shared filesystem instances.

        :param config: an instances of :class objects.ShippingConf:
        """
        directories = self._map_directories(args, shipping_config(config))
        files = docker_remap.walk_files(args, self._remap_and_ship,
                                        directories, pass_dirs=True)
        return self._remove_empty(files)


class ReconstituteBlob(base.Reconstitute):

    """Reconstitute an analysis in a temporary directory on the
    current machine.

    Handles copying or linking files into a work directory,
    running an analysis, then handing off outputs to ship
    back to subsequent processing steps.
    """

    def __init__(self):
        super(ReconstituteBlob, self).__init__()
        self._storage = azure_storage.AzureBlob()

    def _download(self, source, destination):
        """Download file from Azure Blob Storage Service."""
        if os.path.exists(destination):
            return

        download_directory = os.path.dirname(destination)
        utils.safe_makedir(download_directory)
        self._storage.download(filename=source, input_dir=None,
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

        new_args = docker_remap.walk_files(args, _callback,
                                           {suffix: local_dir})
        return (local_dir, new_args)

    def prepare_workdir(self, pack, parallel, args):
        """Unpack necessary files and directories into a temporary structure
        for processing.
        """
        blob_pack = BlobPack()
        workdir, new_args = self._unpack(account_name=pack.storage_account,
                                         container=pack.containers["run"],
                                         args=args)
        datai, data = config_utils.get_dataarg(new_args)
        if "dirs" not in data:
            data["dirs"] = {}
        data["dirs"]["work"] = workdir
        new_args[datai] = data
        return (workdir, new_args, blob_pack.send_run_integrated(pack))

    def get_output(self, target_file, pconfig):
        """Retrieve an output file from pack configuration."""
        blob_name = BLOB_NAME.format(folder=pconfig.folders["output"],
                                     filename=os.path.basename(target_file))
        blob_url = BLOB_FILE.format(storage=pconfig.storage_account,
                                    container=pconfig.containers["run"],
                                    blob=blob_name)

        self._download(source=blob_url, destination=target_file)
        return target_file

    def prepare_datadir(self, pack, args):
        """Prepare the biodata directory.

        :param config: an instances of :class objects.ShippingConf:
        """
        if pack.type == "blob":
            return self._unpack(account_name=pack.storage_account,
                                container=pack.containers["run"],
                                args=args)

        return super(ReconstituteBlob, self).prepare_datadir(pack, args)
