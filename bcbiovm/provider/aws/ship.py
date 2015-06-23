import os

import boto
import toolz
from bcbio import utils
from bcbio.distributed.transaction import file_transaction
from bcbio.pipeline import config_utils

from bcbiovm.common import utils as common_utils
from bcbiovm.docker import remap
from bcbiovm.provider import base


class S3Pack(base.Pack):

    def __init__(self):
        self._conn = boto.connect_s3()

    def _get_bucket(self, bucket_name):
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
        bucket = self._get_bucket(store["bucket"])

        for filename in utils.file_plus_index(orig_fname):
            keyname = "%s/%s" % (store["folder"], os.path.basename(filename))
            key = bucket.get_key(keyname)
            if not key:
                self.upload(filename, keyname, store["bucket"])

        # Drop directory information since we only deal with files in S3
        s3_name = "s3://%s/%s/%s" % (store["bucket"], store["folder"],
                                     os.path.basename(orig_fname))
        return s3_name

    @classmethod
    def upload(cls, filename, key, container):
        """Upload the received file."""
        common_utils.execute(
            ["gof3r", "put", "-p", filename,
             "-k", key, "-b", container,
             "-m", "x-amz-storage-class:REDUCED_REDUNDANCY",
             "-m", "x-amz-server-side-encryption:AES256"],
            check_exit_code=True)

    def send_output(self, config, out_file):
        """Send an output file with state information from a run."""
        keyname = "%s/%s" % (toolz.get_in(["folders", "output"], config),
                             os.path.basename(out_file))
        bucket = toolz.get_in(["buckets", "run"], config)
        self.upload(filename=out_file, key=keyname, container=bucket)


class ReconstituteS3(base.Reconstitute):

    """Reconstitute an analysis in a temporary directory on the
    current machine.

    Handles copying or linking files into a work directory,
    running an analysis, then handing off outputs to ship
    back to subsequent processing steps.
    """

    @staticmethod
    def _download(out_fname, keyname, bucket):
        """Download file from Amazon S3."""
        if os.path.exists(out_fname):
            return

        utils.safe_makedir(os.path.dirname(out_fname))
        with file_transaction(out_fname) as tx_out_fname:
            common_utils.execute(
                ["gof3r", "get", "-p", tx_out_fname,
                 "-k", keyname, "-b", bucket],
                check_exit_code=True)

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
                keyname = fname.replace(remote_key + "/", "")
                self._download(out_fname, keyname, bucket)
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
        self._download(target_file, keyname,
                       toolz.get_in(["buckets", "run"], pconfig))
        return target_file

    def prepare_datadir(self, pack, args):
        """Prepare the biodata directory."""
        if pack["type"] == "S3":
            return self._unpack(pack["buckets"]["biodata"], args)

        return super(ReconstituteS3, self).prepare_datadir(pack, args)
