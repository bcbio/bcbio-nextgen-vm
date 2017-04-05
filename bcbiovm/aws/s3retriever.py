"""Retrieval of resources from AWS S3 buckets.
"""
import functools
import os

from bcbio.distributed import objectstore

# ## S3 specific support

def _config_folders(config):
    for folder in config["folders"]:
        if "/" in folder:
            bucket, rest = folder.split("/", 1)
        else:
            bucket = folder
            rest = ""
        yield "s3://%s@%s/%s" % (bucket, config["region"], rest)

def _find_file(config, target_file):
    for folder in _config_folders(config):
        cur = os.path.join(folder, target_file)
        remote = objectstore.list(cur)
        if cur in remote:
            return cur

# ## API: General functionality

def file_size(file_ref, config=None):
    """Retrieve file size in Mb
    """
    raise NotImplementedError


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
    return _fill_remote(items, functools.partial(_find_file, config))

# ## API: Retrieve files from reference collections

def get_refs(genome_build, aligner, config):
    """Retrieve reference genome data from a standard bcbio directory structure.
    """
    raise NotImplementedError

def get_resources(genome_build, fasta_ref, data):
    """Add genome resources defined in configuration file to data object.
    """
    raise NotImplementedError

# ## Supporting functions

def _fill_remote(cur, find_fn):
    """Add references in data dictionary to remote files if present and not local.
    """
    if isinstance(cur, (list, tuple)):
        return [_fill_remote(x, find_fn) for x in cur]
    elif isinstance(cur, dict):
        out = {}
        for k, v in cur.items():
            out[k] = _fill_remote(v, find_fn)
        return out
    elif isinstance(cur, basestring) and os.path.splitext(cur)[-1] and not os.path.exists(cur):
        remote_cur = find_fn(cur)
        if remote_cur:
            return remote_cur
        else:
            return cur
    else:
        return cur
