"""
Prepare a running process to execute remotely, moving files as necessary
to shared infrastructure.
"""

import os

import boto
from boto.exception import S3ResponseError
import toolz as tz

from bcbio.pipeline import config_utils
from bcbio import utils

from bcbiovm.common import utils as common_utils
from bcbiovm.docker import remap


def shared_filesystem(workdir, datadir, tmpdir=None):
    """Enable running processing within an optional temporary directory.

    workdir is assumed to be available on a shared filesystem, so we don't
    require any work to prepare.
    """
    return {
        "type": "shared",
        "workdir": workdir,
        "tmpdir": tmpdir,
        "datadir": datadir
    }


def prep_s3(biodata_bucket, run_bucket, output_folder):
    """Prepare configuration for shipping to S3.
    """
    return {
        "type": "S3",
        "buckets": {
            "run": run_bucket,
            "biodata": biodata_bucket,
        },
        "folders": {
            "output": output_folder,
        }
    }


def send_run(args, config):
    if config.get("type") == "S3":
        return to_s3(args, config)
    else:
        raise NotImplementedError("Do not yet support pack type: %s", config)


def send_run_integrated(config):
    """Integrated implementation sending run results back to central store.
    """
    def _do(args):
        out = []
        for arg_set in args:
            new_args = send_run(arg_set, config)
            out.append(new_args)
        return out
    return _do


def send_output(config, out_file):
    """Send an output file with state information from a run."""
    if config.get("type") == "S3":
        keyname = "%s/%s" % (tz.get_in(["folders", "output"], config),
                             os.path.basename(out_file))
        bucket = tz.get_in(["buckets", "run"], config)
        _put_s3(out_file, keyname, bucket)
    else:
        pass


def to_s3(args, config):
    """Ship required processing files to S3 for running on non-shared
    filesystem Amazon instances.
    """
    dir_to_s3 = _prep_s3_directories(args, config["buckets"])
    conn = boto.connect_s3()
    args = _remove_empty(remap.walk_files(args, _remap_and_ship(conn),
                                          dir_to_s3, pass_dirs=True))
    return args


def _remove_empty(xs):
    """Remove null values in a nested set of arguments, eliminates unpassed
    values in S3.
    """
    if isinstance(xs, (list, tuple)):
        out = []
        for x in xs:
            item = _remove_empty(x)
            if item is not None:
                out.append(item)
        return out
    elif isinstance(xs, dict):
        out = {}
        for k, v in xs.items():
            v = _remove_empty(v)
            if v is not None:
                out[k] = v
        return out if out else None
    else:
        return xs


def _put_s3(fname, keyname, bucket):
    common_utils.execute(
        ["gof3r", "put", "-p", fname, "-k", keyname,
         "-b", bucket, "-m", "x-amz-storage-class:REDUCED_REDUNDANCY",
         "-m", "x-amz-server-side-encryption:AES256"],
        check_exit_code=True)


def _get_s3_bucket(conn, bucket_name):
    try:
        bucket = conn.get_bucket(bucket_name)
    except S3ResponseError as exc:
        if exc.status == 404:
            bucket = conn.create_bucket(bucket_name)
        else:
            raise
    return bucket


def _remap_and_ship(conn):
    """Remap a file into an S3 bucket and key, shipping if not present.

    Uploads files if not present in the specified bucket, using server
    side encryption.
    Uses gof3r for parallel multipart upload.
    """

    def _work(orig_fname, context, remap_dict):
        # FIXME(alexandrucoman): Unused argument 'context'
        # pylint: disable=unused-argument

        if os.path.isfile(orig_fname):
            dirname = os.path.normpath(os.path.dirname(
                os.path.abspath(orig_fname)))
            store = remap_dict[dirname]
            bucket = _get_s3_bucket(conn, store["bucket"])
            for fname in utils.file_plus_index(orig_fname):
                keyname = "%s/%s" % (store["folder"], os.path.basename(fname))
                key = bucket.get_key(keyname)
                if not key:
                    _put_s3(fname, keyname, store["bucket"])
            s3_name = "s3://%s/%s/%s" % (store["bucket"], store["folder"],
                                         os.path.basename(orig_fname))
        # Drop directory information since we only deal with files in S3
        else:
            s3_name = None
        return s3_name
    return _work


def _prep_s3_directories(args, buckets):
    """Map input directories into stable S3 buckets and folders for
    storing files.
    """
    dirs = set([])

    def _get_dirs(fname, context, remap_dict):
        # FIXME(alexandrucoman): Unused argument 'context' and 'remap_dict'
        # pylint: disable=unused-argument
        dirs.add(os.path.normpath(os.path.dirname(os.path.abspath(fname))))

    remap.walk_files(args, _get_dirs, {}, pass_dirs=True)
    work_dir, biodata_dir = _get_known_dirs(args)
    out = {}
    external_count = 0
    for d in sorted(dirs):
        if work_dir and d.startswith(work_dir):
            folder = d.replace(work_dir, "")
            folder = folder[1:] if folder.startswith("/") else folder
            out[d] = {"bucket": buckets["run"],
                      "folder": folder}
        elif biodata_dir and d.startswith(biodata_dir):
            folder = d.replace(biodata_dir, "")
            folder = folder[1:] if folder.startswith("/") else folder
            out[d] = {"bucket": buckets["biodata"],
                      "folder": folder}
        else:
            folder = os.path.join("externalmap", str(external_count))
            out[d] = {"bucket": buckets["run"],
                      "folder": folder}
            external_count += 1
    return out


def _get_known_dirs(args):
    """Retrieve known local work directory and biodata directories
    as baselines for buckets.
    """
    _, data = config_utils.get_dataarg(args)
    work_dir = tz.get_in(["dirs", "work"], data)
    if "alt" in data["reference"]:
        if data["reference"]["alt"].keys() != [data["genome_build"]]:
            raise NotImplementedError("Need to support packing alternative"
                                      " references to S3")

    parts = tz.get_in(["reference", "fasta", "base"], data).split(os.path.sep)
    while len(parts) > 0:
        last_part = parts.pop(-1)
        if last_part == data["genome_build"]:
            break
    if len(parts) > 0:
        biodata_dir = os.path.sep.join(parts)
    return work_dir, biodata_dir
