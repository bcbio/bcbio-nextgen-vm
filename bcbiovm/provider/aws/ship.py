"""Prepare a running process to execute remotely and reconstitute
an analysis in a temporary directory on the current machine.
"""
import os

import toolz
from bcbio import utils
from bcbio.pipeline import config_utils

from bcbiovm.common import objects
from bcbiovm.docker import remap
from bcbiovm.provider import base
from bcbiovm.provider import objectstore


def shiping_config(biodata_bucket, run_bucket, output_folder):
    """Prepare configuration for shipping to S3."""
    s3_config = objects.ShipingConfig()
    s3_config.add_item("type", "S3")

    s3_config.add_container(name="buckets", alias="containers")
    s3_config.add_item("run", run_bucket, container="buckets")
    s3_config.add_item("biodata", biodata_bucket, container="buckets")

    s3_config.add_container(name="folders")
    s3_config.add_item("output", output_folder, container="folders")
    return s3_config


class S3Pack(base.Pack):

    """Prepare a running process to execute remotely, moving files
    as necessary to shared infrastructure.
    """

    def __init__(self):
        self._storage = objectstore.AmazonS3()

    def _remap_and_ship(self, orig_fname, context, remap_dict):
        """Remap a file into an S3 bucket and key, shipping if not present.

        Uploads files if not present in the specified bucket, using server
        side encryption. Uses gof3r for parallel multipart upload.
        """
        # pylint: disable=unused-argument
        if not os.path.isfile(orig_fname):
            return None

        dirname = os.path.dirname(os.path.abspath(orig_fname))
        store = remap_dict[os.path.normpath(dirname)]

        for filename in utils.file_plus_index(orig_fname):
            keyname = "%s/%s" % (store["folder"], os.path.basename(filename))
            if not self._storage.exists(store["bucket"], keyname):
                self._storage.upload(filename=filename, key=keyname,
                                     container=store["bucket"])

        # Drop directory information since we only deal with files in S3
        s3_name = "s3://%s/%s/%s" % (store["bucket"], store["folder"],
                                     os.path.basename(orig_fname))
        return s3_name

    def send_output(self, config, out_file):
        """Send an output file with state information from a run."""
        keyname = "%s/%s" % (toolz.get_in(["folders", "output"], config),
                             os.path.basename(out_file))
        self._storage.upload(
            filename=out_file, key=keyname,
            container=toolz.get_in(["buckets", "run"], config))


class ReconstituteS3(base.Reconstitute):

    """Reconstitute an analysis in a temporary directory on the
    current machine.

    Handles copying or linking files into a work directory,
    running an analysis, then handing off outputs to ship
    back to subsequent processing steps.
    """

    def __init__(self):
        super(ReconstituteS3, self).__init__()
        self._storage = objectstore.AmazonS3()
        self._s3_url = "s3://{bucket}{region}/{key}"

    def _download(self, source, destination):
        """Download file from Amazon S3."""
        if os.path.exists(destination):
            return

        download_directory = os.path.dirname(destination)
        utils.safe_makedir(download_directory)
        self._storage.download(filename=source, input_dir=None,
                               dl_dir=download_directory)

    def _unpack(self, bucket, args):
        """Create local directory in current directory with pulldowns
        from S3.
        """
        local_dir = utils.safe_makedir(os.path.join(os.getcwd(), bucket))
        remote_key = "s3://%s" % bucket

        def _callback(orig_fname, context, remap_dict):
            """Pull down s3 published data locally for processing."""
            # pylint: disable=unused-argument
            if not orig_fname.startswith(remote_key):
                return orig_fname

            if context[0] in ["reference", "genome_resources", "sam_ref"]:
                cur_dir = os.path.join(local_dir, "genomes")
            else:
                cur_dir = local_dir

            for fname in utils.file_plus_index(orig_fname):
                out_fname = fname.replace(remote_key, cur_dir)
                self._download(source=fname, destination=out_fname)

            return orig_fname.replace(remote_key, cur_dir)

        new_args = remap.walk_files(args, _callback, {remote_key: local_dir})
        return local_dir, new_args

    def prepare_workdir(self, pack, parallel, args):
        """Unpack necessary files and directories into a temporary structure
        for processing.
        """
        s3pack = S3Pack()
        workdir, new_args = self._unpack(pack["buckets"]["run"], args)
        datai, data = config_utils.get_dataarg(new_args)
        if "dirs" not in data:
            data["dirs"] = {}
        data["dirs"]["work"] = workdir
        new_args[datai] = data
        return workdir, new_args, s3pack.send_run_integrated(pack)

    def get_output(self, target_file, pconfig):
        """Retrieve an output file from pack configuration."""
        keyname = "%s/%s" % (toolz.get_in(["folders", "output"], pconfig),
                             os.path.basename(target_file))
        s3_file = self._s3_url.format(
            bucket=toolz.get_in(["buckets", "run"], pconfig),
            region="", key=keyname)
        self._download(source=s3_file, destination=target_file)
        return target_file

    def prepare_datadir(self, pack, args):
        """Prepare the biodata directory."""
        if pack["type"] == "S3":
            return self._unpack(pack["buckets"]["biodata"], args)

        return super(ReconstituteS3, self).prepare_datadir(pack, args)
