"""Prepare a running process to execute remotely and reconstitute
an analysis in a temporary directory on the current machine.
"""
import os
import re

import toolz
from bcbio import utils
from bcbio.pipeline import config_utils

from bcbiovm.common import objects
from bcbiovm.common import objectstore
from bcbiovm.docker import remap
from bcbiovm.provider import base

BLOB_NAME = "{folder}/{filename}"
BLOB_FILE = ("https://{storage}.blob.core.windows.net/"
             "{container}/{blob}")


def get_shiping_config(biodata_container, run_container, output_folder,
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


def shiping_config(config):
    """Create a ShipingConfig object with the received information."""
    blob_config = objects.ShipingConfig(config)
    blob_config.add_alias(container="blobs", alias="containers")
    return blob_config


class BlobPack(base.Pack):

    """Prepare a running process to execute remotely, moving files
    as necessary to shared infrastructure.
    """

    def __init__(self):
        super(BlobPack, self).__ini__()
        self._storage = objectstore.AzureBlob()

    def _upload_if_not_exists(self, filename, store):
        """Upload the received file if not exists."""
        account = store["storage_account"]
        container = store["container"]
        blob_name = BLOB_NAME.format(folder=store["folder"],
                                     filename=os.path.basename(filename))

        if not self._storage.exists(account, container, blob_name):
            self._storage.upload(filename, account, container, blob_name)

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
        self._storage.upload(out_file, account_name, container, blob_name)


class ReconstituteBlob(base.Reconstitute):

    """Reconstitute an analysis in a temporary directory on the
    current machine.

    Handles copying or linking files into a work directory,
    running an analysis, then handing off outputs to ship
    back to subsequent processing steps.
    """

    def __init__(self):
        super(ReconstituteBlob, self).__init__()
        self._storage = objectstore.AzureBlob()

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
