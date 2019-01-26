"""Retrieval of resources from AWS S3 buckets.
"""
import functools
import os

import toolz as tz

from bcbio.distributed import objectstore
from bcbiovm.shared import retriever as sret
from bcbiovm.gcp import retriever as gcp_retriever

# ## S3 specific support

KEY = "s3"

def _config_folders(config):
    ref = [config["ref"]] if "ref" in config else []
    for folder in config.get("folders", []) + config.get("inputs", []) + ref:
        if "/" in folder:
            bucket, rest = folder.split("/", 1)
        else:
            bucket = folder
            rest = ""
        if config.get("region") and not _is_remote(bucket):
            yield "%s://%s@%s/%s" % (KEY, bucket, config["region"], rest)
        else:
            yield folder

def _get_remote_files(config):
    """Retrieve remote file references.
    """
    if "cache" in config:
        return config["cache"]
    out = []
    for f in _config_folders(config):
        out.extend(objectstore.list(f))
    return out

def _is_remote(path):
    return path.startswith("%s:/" % KEY)


_find_file = gcp_retriever._find_file

# ## API: General functionality

def set_cache(config):
    config["cache"] = _get_remote_files(config)
    return config

def file_size(file_ref, config=None):
    """Retrieve file size in Mb.
    """
    conn = objectstore.connect(file_ref)
    remote = objectstore.parse_remote(file_ref)
    bucket = conn.get_bucket(remote.bucket)
    key = bucket.lookup(remote.key)
    return key.size / (1024.0 * 1024.0)

def file_exists(file_ref, config):
    """Check for existence of a remote file, returning path if present
    """
    conn = objectstore.connect(file_ref)
    remote = objectstore.parse_remote(file_ref)
    bucket = conn.get_bucket(remote.bucket)
    key = bucket.lookup(remote.key)
    if key:
        return file_ref

def clean_file(f, config):
    """Remove AWS @-based region specification from file.

    Tools such as Toil use us-east-1 bucket lookup, then pick region
    from boto.
    """
    approach, rest = f.split("://")
    bucket_region, key = rest.split("/", 1)
    if bucket_region.find("@") > 0:
        bucket, region = bucket_region.split("@")
    else:
        bucket = bucket_region
    if config.get("input_type") in ["http", "https"]:
        return "https://s3.amazonaws.com/%s/%s" % (bucket, key)
    else:
        return "%s://%s/%s" % (approach, bucket, key)

# ## API: Fill in files from S3 buckets

def get_files(target_files, config):
    """Retrieve files associated with the potential inputs.
    """
    out = []
    find_fn = _find_file(config)
    for fname_in in target_files.keys():
        if isinstance(fname_in, (list, tuple)):
            fnames = fname_in
        else:
            fnames = fname_in.split(";")
        for fname in fnames:
            remote_fname = find_fn(fname)
            if remote_fname:
                if isinstance(remote_fname, (list, tuple)):
                    out.extend(remote_fname)
                else:
                    out.append(remote_fname)
    return out

def add_remotes(items, config):
    """Add remote files to data, retrieving any files not present locally.
    """
    return sret.fill_remote(items, _find_file(config), _is_remote)

# ## API: Retrieve files from reference collections

def get_refs(genome_build, aligner, config):
    """Retrieve reference genome data from a standard bcbio directory structure.
    """
    find_fn = _find_file(config[KEY], prefix=config[KEY]["ref"])
    ref_prefix = sret.find_ref_prefix(genome_build, find_fn)
    return sret.standard_genome_refs(genome_build, aligner, ref_prefix, objectstore.list)

def get_resources(genome_build, fasta_ref, data):
    """Add genome resources defined in configuration file to data object.
    """
    return sret.get_resources(genome_build, fasta_ref, tz.get_in(["config", KEY], data),
                              data, objectstore.open_file, objectstore.list)
