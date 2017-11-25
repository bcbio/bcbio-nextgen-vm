"""Integration with SevenBridges and Cancer Genomics Cloud, using the API
"""
import contextlib
import os
import tempfile
import shutil

import toolz as tz

from bcbiovm.shared import retriever as sret

# ## Seven Bridges specific functionality

KEY = "sbg"
CONFIG_KEY = "sbgenomics"

def _get_api_client():
    assert os.environ.get("CGC_API_URL") and os.environ.get("CGC_AUTH_TOKEN"), \
        "Need to set CGC_API_URL and CGC_AUTH_TOKEN to retrieve files from the Seven Bridges Platform"
    import sevenbridges as sbg
    api = sbg.Api(os.environ["CGC_API_URL"], os.environ["CGC_AUTH_TOKEN"])
    return api

def _is_remote(f):
    return f.startswith("%s:" % KEY)

def _project_files(project_name):
    """Retrieve files in the input project.

    Uses special bcbio_path metadata object from upload to handle
    nested files in reference directories, since SevenBridges does not
    yet allow folders.
    """
    api = _get_api_client()
    project = [p for p in api.projects.query(limit=None).all() if p.id == project_name][0]
    files = list(api.files.query(project).all())

    out = []
    for api_file in files:
        fname = os.path.join(api_file.metadata.get("bcbio_path", ""), api_file.name)
        out.append((fname, api_file.id))
    return out

def _get_remote_files(config):
    """Retrieve remote file references.
    """
    out = []
    for pname in [config["project"], config["reference"]]:
        out += _project_files(pname)
    return out

def _get_id_fname(file_ref):
    return file_ref.split(":")[-1].split("/", 1)

@contextlib.contextmanager
def _open_remote(file_ref):
    """Retrieve an open handle to a file.
    """
    api = _get_api_client()
    fid, fname = _get_id_fname(file_ref)
    api_file = api.files.get(id=fid)
    temp_dir = tempfile.mkdtemp()
    dl_file = os.path.join(temp_dir, os.path.basename(fname))
    api_file.download(dl_file)
    with open(dl_file) as in_handle:
        yield in_handle
    shutil.rmtree(temp_dir)

def _find_file(config, startswith=False):
    remote_files = _get_remote_files(config)
    def get_file(f):
        if _is_remote(f):
            f = _get_id_fname(f)[-1]
        for fname, fid in remote_files:
            if fname == f:
                return "%s:%s/%s" % (KEY, fid, fname)
            elif startswith and fname.startswith(f):
                return "%s:%s/%s" % (KEY, fid, f)
    return get_file

def _list(config):
    remote_files = _get_remote_files(config)
    def do(d):
        out = []
        dfname = _get_id_fname(d)[-1]
        for fname, fid in remote_files:
            if fname.startswith(dfname):
                out.append("%s:%s/%s" % (KEY, fid, fname))
        return out
    return do

# ## API: General functionality

def set_cache(config):
    return config

def file_size(file_ref, config=None):
    api = _get_api_client()
    api_file = api.files.get(id=_get_id_fname(file_ref)[0])
    return api_file.size

def file_exists(file_ref, config):
    """Check for existence of a remote file, returning path if present
    """
    find_fn = _find_file(config)
    if _is_remote(file_ref):
        _, file_ref = _get_id_fname(file_ref)
    return find_fn(file_ref)

def clean_file(f):
    """Return only the SBG ID for referencing in the JSON.
    """
    return _get_id_fname(f)[0]

# ## API: Fill in files from S3 buckets

def get_files(target_files, config):
    """Retrieve files associated with the potential inputs.
    """
    out = []
    find_fn = _find_file(config)
    for fname in target_files.keys():
        remote_fname = find_fn(fname)
        if remote_fname:
            out.append(remote_fname)
    return out

def add_remotes(items, config):
    """Add remote files to data, retrieving any files not present locally.
    """
    find_fn = _find_file(config)
    return sret.fill_remote(items, find_fn, _is_remote)

# ## API: Retrieve files from reference collections

def get_refs(genome_build, aligner, config):
    """Retrieve reference genome data from a standard bcbio directory structure.
    """
    find_fn = _find_file(config[CONFIG_KEY], startswith=True)
    ref_prefix = sret.find_ref_prefix(genome_build, find_fn)
    return sret.standard_genome_refs(genome_build, aligner, ref_prefix, _list(config[CONFIG_KEY]))

def get_resources(genome_build, fasta_ref, data):
    """Add genome resources defined in configuration file to data object.
    """
    config = tz.get_in(["config", CONFIG_KEY], data)
    find_fn = _find_file(config)
    def normalize(f):
        return _get_id_fname(f)[-1]
    return sret.get_resources(genome_build, fasta_ref, config,
                              data, _open_remote, _list(config), find_fn, normalize)
