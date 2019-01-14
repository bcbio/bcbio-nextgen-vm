"""Integration with Arvados Keep. using the API with arvados-python-sdk.
"""
import os

import six
import toolz as tz

from bcbiovm.shared import retriever as sret

# ## Arvados specific functionality

KEY = "keep"
CONFIG_KEY = "arvados"

def _get_api_client(config=None):
    if not config: config = {}
    if "token" in config and "host" in config:
        os.environ["ARVADOS_API_HOST"] = config["host"]
        os.environ["ARVADOS_API_TOKEN"] = config["token"]
    assert os.environ.get("ARVADOS_API_HOST") and os.environ.get("ARVADOS_API_TOKEN"), \
        "Need to set ARVADOS_API_HOST and ARVADOS_API_TOKEN to retrieve files from Keep"
    import arvados
    return arvados.api("v1")

def _get_input_ids(config):
    """Retrieve input IDs for collections, normalizing to a list.
    """
    ref_uuid = config.get("reference") or config.get("ref")
    out = [ref_uuid] if ref_uuid else []
    input_id = config.get("input") or config.get("inputs")
    if not input_id:
        return out
    elif isinstance(input_id, six.string_types):
        return sorted(list(set(out + [input_id])))
    else:
        assert isinstance(input_id, (list, tuple)), input_id
        return sorted(list(set(out + input_id)))

def _is_remote(f):
    return f.startswith("%s:" % KEY)

def _collection_files(uuid, config):
    """Retrieve files in the input collection.
    """
    import arvados
    api_client = _get_api_client(config)
    cr = arvados.CollectionReader(uuid, api_client=api_client)
    cr.normalize()
    pdh = cr.portable_data_hash()
    out = [str("%s:%s/%s" % (KEY, os.path.normpath(os.path.join(pdh, x.stream_name())), x.name))
           for x in cr.all_files()]
    return out

def _get_remote_files(config):
    """Retrieve remote file references.
    """
    if "cache" in config:
        return config["cache"]
    out = []
    for input_id in _get_input_ids(config):
        out += _collection_files(input_id, config)
    return out

def _get_uuid_file(file_ref):
    return file_ref.replace("%s:" % KEY, "").split("/", 1)

def _open_remote(file_ref, config=None):
    """Retrieve an open handle to a file in an Arvados Keep collection.
    """
    import arvados
    api_client = _get_api_client(config)
    coll_uuid, coll_ref = _get_uuid_file(file_ref)
    cr = arvados.CollectionReader(coll_uuid, api_client=api_client)
    return cr.open(coll_ref)

def _find_file(config, startswith=False):
    """Flexibly search for files in an Arvados collection.

    startswith -- searching for directories
    """
    keep_files = _get_remote_files(config)
    def get_file(f):
        # exact matches
        for keep_full in keep_files:
            keep_uuid, keep_file = _get_uuid_file(keep_full)
            if keep_file == f:
                return keep_full
            elif startswith and keep_file.startswith(f):
                return "%s:%s/%s" % (KEY, keep_uuid, f)
        # partial matches, including directories (using startswith)
        for keep_full in keep_files:
            keep_uuid, keep_file = _get_uuid_file(keep_full)
            if startswith:
                keep_file = os.path.dirname(keep_file)
            if keep_file.endswith("/%s" % f):
                return "%s:%s/%s" % (KEY, keep_uuid, keep_file)
    return get_file

def _list(config):
    keep_files = _get_remote_files(config)
    def do(d):
        out = []
        for keep_full in keep_files:
            if keep_full.startswith(d):
                out.append(keep_full)
        return out
    return do

# ## API: General functionality

def file_size(file_ref, config=None):
    """Retrieve file size in keep, in Mb
    """
    import arvados
    api_client = _get_api_client(config)
    coll_uuid, coll_ref = _get_uuid_file(file_ref)
    cr = arvados.CollectionReader(coll_uuid, api_client=api_client)
    file = cr.find(coll_ref)
    return file.size() / (1024.0 * 1024.0)

def file_exists(file_ref, config):
    """Check for existence of a remote file, returning path if present
    """
    find_fn = _find_file(config)
    if _is_remote(file_ref):
        _, file_ref = _get_uuid_file(file_ref)
    return find_fn(file_ref)

def clean_file(f, config):
    return f

# ## API

def set_cache(config):
    """Add cache of files to avoid multiple API lookups.
    """
    config["cache"] = _get_remote_files(config)
    return config

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
    return sret.get_resources(genome_build, fasta_ref, config,
                              data, _open_remote, _list(config))
