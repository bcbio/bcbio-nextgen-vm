"""Integration with the DNAnexus platform using the API.

Looks up and fills in sample locations from inputs folders in a DNAnexus project.
"""
import fnmatch
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
    return file_ref.split(":", 1)[-1].split("/", 1)

def _recursive_ls(dx_proj, project_name, folder):
    out = {}
    try:
        query = dx_proj.list_folder(folder=folder, describe={"fields": {'name': True}})
    except dxpy.exceptions.ResourceNotFound:
        print(dx_proj, folder)
        raise
    for subfolder in query["folders"]:
        out.update(_recursive_ls(dx_proj, project_name, subfolder))
    for f in query["objects"]:
        out[str(os.path.join(folder, f["describe"]["name"]))] = (project_name, str(f["id"]))
    return out

def _project_files(project_name, folder):
    """Retrieve files in the input project and folder.
    """
    _authenticate()
    if project_name.startswith("project-"):
        project_id = project_name
    else:
        query = dxpy.api.system_find_projects({"name": project_name, "level": "VIEW"})
        if len(query["results"]) == 1:
            project_id = query["results"][0]["id"]
        else:
            raise ValueError("Did not find DNAnexus project %s: %s" % (project_name, query))
    dx_proj = dxpy.get_handler(project_id)
    return _recursive_ls(dx_proj, project_name, folder)

def _remote_folders(config):
    if isinstance(config["ref"], dict):
        ref_folder = (config["ref"].get("project") or config["project"], config["ref"]["folder"])
    else:
        ref_folder = (config["project"], config["ref"])
    return [ref_folder] + [(config["project"], f) for f in config["inputs"]]

def _get_remote_files(config):
    """Retrieve remote file references.
    """
    if "cache" in config:
        return config["cache"]
    out = {}
    for project, folder in _remote_folders(config):
        out.update(_project_files(project, folder))
    return out

def _open_remote(file_ref):
    """Retrieve an open handle to a file.
    """
    _authenticate()
    return dxpy.bindings.dxfile.DXFile(_get_id_fname(file_ref)[0])

def _find_file(config, startswith=False):
    """Resolve a file in the remove files.

    startswith allows queries for directories.
    Looks for exact matches then tries to find a file recursively in a folder
    """
    remote_files = _get_remote_files(config)
    if startswith:
        remote_folders = {}
        for fname, (pid, _) in remote_files.items():
            remote_folders[os.path.dirname(fname)] = (pid, None)
        remote_files = remote_folders

    def glob_match(f1, f2):
        """Check for wildcard glob style matches.
        """
        if f1.find("*") >= 0:
            if fnmatch.fnmatch(f2, "*/%s" % f1):
                return True

    def get_file(f):
        if _is_remote(f):
            f = _get_id_fname(f)[-1]
        # handle both bare lookups and project-prefixed
        if f.find(":") > 0:
            fproject, f = f.split(":")
        else:
            fproject = None
        # check for exact matches
        for project, folder in _remote_folders(config):
            if fproject is None or fproject == project:
                folder_f = os.path.join(folder, f)
                if folder_f in remote_files:
                    pid, fid = remote_files[folder_f]
                    return "%s:%s/%s:%s" % (KEY, fid, pid, folder_f)
        # find any files nested in sub folders or as globs
        out = []
        for project, folder in _remote_folders(config):
            for rfname, (pid, rid) in remote_files.items():
                if rfname.startswith(folder + "/") and (rfname.endswith("/" + f) or glob_match(f, rfname)):
                    out.append("%s:%s/%s:%s" % (KEY, rid, pid, rfname))
        if len(out) == 1:
            return out[0]
        elif len(out) > 1:
            return out
    return get_file

def _list(config):
    remote_files = _get_remote_files(config)

    def do(d):
        out = []
        dfname = _get_id_fname(d)[-1]
        for fname, (pid, fid) in remote_files.items():
            if ("%s:%s" % (pid, fname)).startswith(dfname):
                out.append("%s:%s/%s:%s" % (KEY, fid, pid, fname))
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

def file_exists(file_ref, config):
    """Check for existence of a remote file, returning path if present
    """
    find_fn = _find_file(config)
    if _is_remote(file_ref):
        _, file_ref = _get_id_fname(file_ref)
    return find_fn(file_ref)

def clean_file(f):
    # Return full file paths instead of IDs to enable CWL secondary file lookup
    return _get_id_fname(f)[1]

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
