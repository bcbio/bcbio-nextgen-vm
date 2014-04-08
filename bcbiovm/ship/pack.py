"""Prepare a running process to execute remotely, moving files as necessary to shared infrastructure.
"""
import os

from bcbiovm.docker import remap

def shared_filesystem(workdir, datadir, tmpdir=None):
    """Enable running processing within an optional temporary directory.

    workdir is assumed to be available on a shared filesystem, so we don't
    require any work to prepare.
    """
    return {"type": "shared", "workdir": workdir, "tmpdir": tmpdir, "datadir": datadir}

def prep_s3(biodata_bucket, run_bucket):
    """Prepare configuration for shipping to S3.
    """
    return {"type": "S3", "buckets": {"run": run_bucket, "biodata": biodata_bucket}}

def send_run(args, config):
    if config.get("type") == "S3":
        return to_s3(args, config)
    else:
        raise NotImplementedError("Do not yet support pack type: %s", config)

def to_s3(args, config):
    """Ship required processing files to S3 for running on non-shared filesystem Amazon instances.
    """
    import pprint
    pprint.pprint(config)
    pprint.pprint(args)
    _prep_s3_directories(args)
    raise NotImplementedError
    return config

def _prep_s3_directories(args):
    dirs = set([])
    def _get_dirs(fname, context, remap_dict):
        dirs.add(os.path.normpath(os.path.dirname(os.path.abspath(fname))))
    remap.walk_files(args, _get_dirs, {})
    print dirs
