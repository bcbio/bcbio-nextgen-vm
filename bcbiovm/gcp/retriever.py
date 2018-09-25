"""Integration with Google Cloud Storage, using gsutil.
"""
import fnmatch
import io
import os
import sys
import subprocess

import toolz as tz

from bcbiovm.shared import retriever as sret

# ## Google Cloud specific functionality

KEY = "gs"

def _is_remote(f):
    return f.startswith("%s:" % KEY)

def _run_gsutil(args):
    cmd = os.path.join(os.path.dirname(sys.executable), "gsutil")
    return subprocess.check_output([cmd] + args)

def _recursive_ls(bucket):
    out = []
    for l in _run_gsutil(["ls", "-r", bucket]).split("\n"):
        if l.strip() and not l.endswith("/:"):
            out.append(l.strip())
    return out

def _remote_buckets(config):
    return [config["ref"]] + config["inputs"]

def _get_remote_files(config):
    """Retrieve remote file references.
    """
    if "cache" in config:
        return config["cache"]
    out = []
    for b in _remote_buckets(config):
        out.extend(_recursive_ls(b))
    return out

def _open_remote(file_ref):
    """Retrieve an open handle to a file.
    """
    return io.StringIO(unicode(_run_gsutil(["cat", file_ref])))

def _find_file(config, prefix=None):
    """Resolve a file in the remote files.

    prefix allows queries for directories like reference locations.
    Looks for exact matches then tries to find a file recursively in a folder
    """
    remote_files = _get_remote_files(config)

    def glob_match(f1, f2):
        """Check for wildcard glob style matches.
        """
        if f1.find("*") >= 0:
            if fnmatch.fnmatch(f2, "*/%s" % f1):
                return True

    def get_file(f):
        # find any files as prefixes, exact matches or globs
        out = []
        for fname in remote_files:
            if prefix and fname.startswith(os.path.join(prefix, f)):
                out = [os.path.join(prefix, f)]
            elif fname == f or glob_match(f, fname) or fname.endswith("/" + f):
                out.append(fname)
        if len(out) == 1:
            return out[0]
        elif len(out) > 1:
            return out
    return get_file

def _list(config):
    remote_files = _get_remote_files(config)

    def do(d):
        out = []
        for fname in remote_files:
            if fname.startswith(d):
                out.append(fname)
        return out
    return do

# ## API: General functionality

def file_size(file_ref, config=None):
    """Retrieve file size in Mb.
    """
    size_str = _run_gsutil(["du", file_ref])
    return float(size_str.split()[0]) / (1024.0 * 1024.0)

def file_exists(file_ref, config):
    """Check for existence of a remote file, returning path if present
    """
    find_fn = _find_file(config)
    return find_fn(file_ref)

def clean_file(f):
    return f

# ## API: Fill in files

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
    find_fn = _find_file(config)
    return sret.fill_remote(items, find_fn, _is_remote)

# ## API: Retrieve files from reference collections

def set_cache(config):
    """Add a cache to the configuration to prevent multiple downloads
    """
    config["cache"] = _get_remote_files(config)
    return config

def get_refs(genome_build, aligner, config):
    """Retrieve reference genome data from a standard bcbio directory structure.
    """
    find_fn = _find_file(config[KEY], prefix=config[KEY]["ref"])
    ref_prefix = sret.find_ref_prefix(genome_build, find_fn)
    return sret.standard_genome_refs(genome_build, aligner, ref_prefix, _list(config[KEY]))

def get_resources(genome_build, fasta_ref, data):
    """Add genome resources defined in configuration file to data object.
    """
    config = tz.get_in(["config", KEY], data)
    return sret.get_resources(genome_build, fasta_ref, config,
                              data, _open_remote, _list(config))
