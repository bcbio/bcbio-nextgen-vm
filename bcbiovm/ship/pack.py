"""
Prepare a running process to execute remotely, moving files as necessary
to shared infrastructure.
"""
import abc
import os

import boto
import six
import toolz

from bcbio.pipeline import config_utils
from bcbio import utils

from bcbiovm.common import utils as common_utils
from bcbiovm.docker import remap


@six.add_metaclass(abc.ABCMeta)
class Pack(object):

    """Prepare a running process to execute remotely, moving files
    as necessary to shared infrastructure.
    """

    _CONTAINER = "buckets"

    def _remove_empty(self, argument):
        """Remove null values in a nested set of arguments."""
        if isinstance(argument, (list, tuple)):
            output = []
            for item in argument:
                item = self._remove_empty(item)
                if item is not None:
                    output.append(item)
            return output
        elif isinstance(argument, dict):
            output = {}
            for key, value in argument.items():
                value = self._remove_empty(value)
                if value is not None:
                    output[key] = value
            return output if output else None
        else:
            return argument

    @staticmethod
    def _local_directories(args):
        """Retrieve known local work directory and biodata directories
        as baselines for buckets.
        """
        _, data = config_utils.get_dataarg(args)
        work_dir = toolz.get_in(["dirs", "work"], data)
        if "alt" in data["reference"]:
            if data["reference"]["alt"].keys() != [data["genome_build"]]:
                raise NotImplementedError("Need to support packing alternative"
                                          " references.")

        parts = toolz.get_in(["reference", "fasta",
                              "base"], data).split(os.path.sep)
        while parts:
            if parts.pop() == data["genome_build"]:
                break

        biodata_dir = os.path.sep.join(parts) if parts else None
        return (work_dir, biodata_dir)

    def _map_directories(self, args, containers):
        """Map input directories into stable containers and folders for
        storing files.
        """
        output = {}
        external_count = 0
        directories = set()

        def _callback(filename, *args):
            """Callback function for remap.walk_files."""
            # pylint: disable=unused-argument
            directory = os.path.dirname(os.path.abspath(filename))
            directories.add(os.path.normpath(directory))

        remap.walk_files(args, _callback, {}, pass_dirs=True)
        work_dir, biodata_dir = self._local_directories(args)
        for directory in sorted(directories):
            if work_dir and directory.startswith(work_dir):
                folder = directory.replace(work_dir, "").strip("/")
                output[directory] = {"container": containers["run"],
                                     "folder": folder}
            elif biodata_dir and directory.startswith(biodata_dir):
                folder = directory.replace(biodata_dir, "").strip("/")
                output[directory] = {"container": containers["biodata"],
                                     "folder": folder}
            else:
                folder = os.path.join("externalmap", str(external_count))
                output[directory] = {"container": containers["run"],
                                     "folder": folder}
                external_count += 1
        return output

    def send_run_integrated(self, config):
        """Integrated implementation sending run results back
        to central store.
        """

        def finalizer(args):
            output = []
            for arg_set in args:
                new_args = self.send_run(arg_set, config)
                output.append(new_args)
            return output

        return finalizer

    def send_run(self, args, config):
        """Ship required processing files to the storage service for running
        on non-shared filesystem instances.
        """
        directories = self._map_directories(args, config[self._CONTAINER])
        files = remap.walk_files(args, self._remap_and_ship,
                                 directories, pass_dirs=True)
        return self._remove_empty(files)

    @abc.abstractmethod
    def _remap_and_ship(self, orig_fname, context, remap_dict):
        """Uploads files if not present in the specified container.

        Remap a file into an storage service container and key,
        shipping if not present.
        """
        pass

    @abc.abstractmethod
    def upload(self, filename, key, container):
        """Upload the received file."""
        pass

    @abc.abstractmethod
    def send_output(self, config, out_file):
        """Send an output file with state information from a run."""
        pass


class S3Pack(Pack):

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
