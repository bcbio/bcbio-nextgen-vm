"""Retrieval of resources from AWS S3 buckets.
"""
import functools
import os

import toolz as tz

from bcbio.distributed import objectstore
from bcbiovm.shared import retriever as sret

# ## S3 specific support

KEY = "s3"

def _config_folders(config):
    for folder in config["folders"]:
        if "/" in folder:
            bucket, rest = folder.split("/", 1)
        else:
            bucket = folder
            rest = ""
        yield "%s://%s@%s/%s" % (KEY, bucket, config["region"], rest)

def _find_file(config, target_file):
    for folder in _config_folders(config):
        cur = os.path.join(folder, target_file)
        remote = objectstore.list(cur)
        if remote:
            return cur

def _is_remote(path):
    return path.startswith("%s:/" % KEY)

# ## API: General functionality

def set_cache(config):
    return config

def file_size(file_ref, config=None):
    """Retrieve file size in Mb.
    """
    conn = objectstore.connect(file_ref)
    remote = objectstore.parse_remote(file_ref)
    bucket = conn.get_bucket(remote.bucket)
    key = bucket.lookup(remote.key)
    return key.size / (1024.0 * 1024.0)

def clean_file(f):
    """Remove AWS @-based region specification from file.

    Tools such as Toil use us-east-1 bucket lookup, then pick region
    from boto.
    """
    approach, rest = f.split("://")
    bucket_region, key = rest.split("/", 1)
    bucket, region = bucket_region.split("@")
    return "%s://%s/%s" % (approach, bucket, key)

# ## API: Fill in files from S3 buckets

def get_files(target_files, config):
    """Retrieve files associated with the template inputs.
    """
    out = []
    for fname in target_files.keys():
        remote_fname = _find_file(config, fname)
        if remote_fname:
            out.append(remote_fname)
    return out

def add_remotes(items, config):
    """Add remote files to data, retrieving any files not present locally.
    """
    return sret.fill_remote(items, functools.partial(_find_file, config), _is_remote)

# ## API: Retrieve files from reference collections

def get_refs(genome_build, aligner, config):
    """Retrieve reference genome data from a standard bcbio directory structure.
    """
    ref_prefix = sret.find_ref_prefix(genome_build, functools.partial(_find_file, config[KEY]))
    return sret.standard_genome_refs(genome_build, aligner, ref_prefix, objectstore.list)

def get_resources(genome_build, fasta_ref, data):
    """Add genome resources defined in configuration file to data object.
    """
    return sret.get_resources(genome_build, fasta_ref, tz.get_in(["config", KEY], data),
                              data, objectstore.open, objectstore.list)
