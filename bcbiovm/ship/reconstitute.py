"""Reconstitute an analysis in a temporary directory on the current machine.

Handles copying or linking files into a work directory, running an analysis,
then handing off outputs to ship back to subsequent processing steps.
"""
import os
import uuid
import shutil

from bcbio import utils
from bcbiovm.docker import remap

def prep_workdir(pack, args):
    """Unpack necessary files and directories into a temporary structure for processing
    """
    if pack["type"] == "shared":
        workdir, new_args = _create_workdir_shared(pack["workdir"], args, pack["tmpdir"])
        return workdir, new_args, _shared_finalizer(new_args, workdir, pack["workdir"])
    else:
        raise ValueError("Currently only handle shared filesystems")

# ## Shared filesystem

def _create_workdir_shared(workdir, args, tmpdir=None):
    """Create a work directory given inputs from the shared filesystem.

    If tmpdir is not None, we create a local working directory within the
    temporary space so IO and processing occurs there, remapping the input
    argument paths at needed.
    """
    if not tmpdir:
        return workdir, args
    else:
        new_workdir = utils.safe_makedir(os.path.join(tmpdir, "bcbio-work-%s" % uuid.uuid1()))
        new_args = remap.walk_files(args, _remap_copy_file, {workdir: new_workdir})
        return new_workdir, new_args

def _remap_copy_file(fname, orig_to_temp):
    """Remap file names and copy into temporary directory as needed.

    Handles simultaneous transfer of associated indexes.
    """
    new_fname = remap.remap_fname(fname, orig_to_temp)
    if os.path.isfile(fname):
        utils.safe_makedir(os.path.dirname(new_fname))
        for ext in ["", ".idx", ".gbi", ".tbi", ".bai"]:
            if os.path.exists(fname + ext):
                if not os.path.exists(new_fname + ext):
                    shutil.copyfile(fname + ext, new_fname + ext)
    elif os.path.isdir(fname):
        utils.safe_makedir(new_fname)
    return new_fname

def _shared_finalizer(args, workdir, orig_workdir):
    """Cleanup temporary working directory, copying missing files back to the shared workdir.
    """
    def _do(out):
        if workdir != orig_workdir:
            new_out = remap.walk_files(out, _remap_copy_file, {workdir: orig_workdir}) if out else None
            if os.path.exists(workdir):
                shutil.rmtree(workdir)
            return new_out
    return _do
