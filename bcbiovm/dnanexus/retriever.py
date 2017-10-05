"""Integration with the DNAnexus platform using the API.

Looks up and fills in sample locations from inputs folders in a DNAnexus project.
"""
import os

import toolz as tz

from bcbio import utils
from bcbiovm.shared import retriever as sret

dxpy = utils.LazyImport("dxpy")

# ## DNAnexus specific functionality

KEY = "dx"
CONFIG_KEY = "dnanexus"

def _authenticate():
    assert os.environ.get("DX_AUTH_TOKEN"), \
        "Need to set DX_AUTH_TOKEN for file retrieval from DNAnexus"
    dxpy.set_security_context({"auth_token_type": "bearer", "auth_token": os.environ["DX_AUTH_TOKEN"]})

def _is_remote(f):
    return f.startswith("%s:" % KEY)

def _get_id_fname(file_ref):
    return file_ref.split(":")[-1].split("/", 1)

def _recursive_ls(dx_proj, folder):
    out = {}
    query = dx_proj.list_folder(folder=folder, describe={"fields": {'name': True}})
    for subfolder in query["folders"]:
        out.update(_recursive_ls(dx_proj, subfolder))
    for f in query["objects"]:
        out[str(os.path.join(folder, f["describe"]["name"]))] = str(f["id"])
    return out

def _project_files(project_name, folder):
    """Retrieve files in the input project and folder.
    """
    _authenticate()
    if project_name.startswith("project-"):
        project_id = project_name
    else:
        query = dxpy.api.system_find_projects({"name": project_name})
        if len(query["results"]) == 1:
            project_id = query["results"][0]["id"]
        else:
            raise ValueError("Did not find DNAnexus project %s: %s" % (project_name, query))
    dx_proj = dxpy.get_handler(project_id)
    return _recursive_ls(dx_proj, folder)

def _remote_folders(config):
    return [config["ref"]] + config["inputs"]

def _get_remote_files(config):
    """Retrieve remote file references.
    """
    out = {}
    for folder in _remote_folders(config):
        out.update(_project_files(config["project"], folder))
    return out

def _open_remote(file_ref):
    """Retrieve an open handle to a file.
    """
    _authenticate()
    return dxpy.bindings.dxfile.DXFile(_get_id_fname(file_ref)[0])

def _find_file(config, startswith=False):
    remote_files = _get_remote_files(config)
    if startswith:
        remote_folders = {}
        for fname in remote_files.keys():
            remote_folders[os.path.dirname(fname)] = None
        remote_files = remote_folders
    def get_file(f):
        if _is_remote(f):
            f = _get_id_fname(f)[-1]
        for folder in _remote_folders(config):
            folder_f = os.path.join(folder, f)
            if folder_f in remote_files:
                return "%s:%s/%s" % (KEY, remote_files[folder_f], folder_f)
    return get_file

def _list(config):
    remote_files = _get_remote_files(config)
    def do(d):
        out = []
        dfname = _get_id_fname(d)[-1]
        for fname, fid in remote_files.items():
            if fname.startswith(dfname):
                out.append("%s:%s/%s" % (KEY, fid, fname))
        return out
    return do

# ## API: General functionality

def file_size(file_ref, config=None):
    """Retrieve file size in Mb.
    """
    _authenticate()
    file_id = _get_id_fname(file_ref)[0]
    dx_file = dxpy.get_handler(file_id)
    desc = dx_file.describe(fields={"size": True})
    return desc["size"] / (1024.0 * 1024.0)

def clean_file(f):
    # Return full file paths instead of IDs to enable CWL secondary file lookup
    return _get_id_fname(f)[1]

# ## API: Fill in files

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
