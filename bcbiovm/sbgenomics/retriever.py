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

def _get_api_client(config):
    import sevenbridges as sbg
    c = sbg.Config(profile=config.get("profile", "default"))
    api = sbg.Api(config=c, advance_access=True)
    return api

def _is_remote(f):
    return f.startswith("%s:" % KEY)

def _recursive_list(api, parent, base_folder=""):
    """Enumerate all files, including nesting, under a parent folder.
    """
    out = []
    for f in api.files.query(parent=parent):
        if f.type == "folder":
            out += _recursive_list(api, f.id, os.path.join(base_folder, f.name))
        else:
            out.append((os.path.join(base_folder, f.name), f))
    return out

def _find_parent(api, project, name):
    """Find a parent folder to enumerate inputs under.
    """
    cur_folder = None
    for f in [x for x in name.split("/") if x]:
        if not cur_folder:
            cur_folder = list(api.files.query(project, names=[f]).all())[0]
        else:
            cur_folder = list(api.files.query(parent=cur_folder.id, names=[f]).all())[0]
    return cur_folder

def _project_files(project_name, folder, config):
    """Retrieve files in the input project.
    """
    api = _get_api_client(config)
    project = [p for p in api.projects.query(limit=None, name=os.path.basename(project_name)).all()
               if p.id.endswith(project_name)][0]
    sb_folder = _find_parent(api, project, folder)
    out = []
    for full_path, api_file in _recursive_list(api, sb_folder.id):
        out.append((full_path, api_file.id))
    return out

def _get_remote_files(config):
    """Retrieve remote file references.

    TODO: generalize for reference inputs in alternative projects, but
    might not be practical in SBG.
    """
    if "cache" in config:
        return config["cache"]
    out = []
    for pname in [config["project"], config.get("ref", config.get("reference"))]:
        if pname:
            for folder in config["inputs"]:
                out += _project_files(pname, folder, config)
    return out

def _get_id_fname(file_ref):
    return file_ref.split(":")[-1].split("/", 1)

def _open_remote(config):
    @contextlib.contextmanager
    def _do(file_ref):
        """Retrieve an open handle to a file.
        """
        api = _get_api_client(config)
        fid, fname = _get_id_fname(file_ref)
        api_file = api.files.get(id=fid)
        temp_dir = tempfile.mkdtemp()
        dl_file = os.path.join(temp_dir, os.path.basename(fname))
        api_file.download(dl_file)
        with open(dl_file) as in_handle:
            yield in_handle
        shutil.rmtree(temp_dir)
    return _do

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
    config["cache"] = _get_remote_files(config)
    return config

def file_size(file_ref, config=None):
    api = _get_api_client(config)
    api_file = api.files.get(id=_get_id_fname(file_ref)[0])
    return api_file.size

def file_exists(file_ref, config):
    """Check for existence of a remote file, returning path if present
    """
    find_fn = _find_file(config)
    if _is_remote(file_ref):
        _, file_ref = _get_id_fname(file_ref)
    return find_fn(file_ref)

def clean_file(f, config):
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
    if config.get(KEY):
        config = config[KEY]
    elif config.get(CONFIG_KEY):
        config = config[CONFIG_KEY]
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
                              data, _open_remote(config), _list(config), find_fn, normalize)
