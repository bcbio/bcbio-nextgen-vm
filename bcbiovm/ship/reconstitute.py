"""Reconstitute an analysis in a temporary directory on the current machine.

Handles copying or linking files into a work directory, running an analysis,
then handing off outputs to ship back to subsequent processing steps.
"""
import os
import shutil
import subprocess
import uuid

import toolz as tz
import yaml

from bcbio.distributed.transaction import file_transaction
from bcbio.log import logger
from bcbio.pipeline import config_utils
from bcbio import utils
from bcbiovm.docker import remap
from bcbiovm.ship import pack as ship_n_pack


def prep_workdir(pack, parallel, args):
    """Unpack necessary files and directories into a temporary structure
    for processing.
    """
    if pack["type"] == "shared":
        workdir, remap_dict, new_args = _create_workdir_shared(
            pack["workdir"], args, parallel, pack["tmpdir"])
        return workdir, new_args, _shared_finalizer(new_args, workdir,
                                                    remap_dict, parallel)

    elif pack["type"] == "S3":
        workdir, new_args = _unpack_s3(pack["buckets"]["run"], args)
        datai, data = config_utils.get_dataarg(new_args)
        if "dirs" not in data:
            data["dirs"] = {}
        data["dirs"]["work"] = workdir
        new_args[datai] = data
        return workdir, new_args, ship_n_pack.send_run_integrated(pack)
    else:
        raise ValueError("Cannot handle work directory "
                         "preparation type: %s" % pack)


def prep_datadir(pack, args):
    if "datadir" in pack:
        return pack["datadir"], args
    elif pack["type"] == "S3":
        return _unpack_s3(pack["buckets"]["biodata"], args)
    else:
        raise ValueError("Cannot handle biodata directory "
                         "preparation type: %s" % pack)


def prep_systemconfig(datadir, args):
    """Prepare system configuration files on bare systems if not present.
    """
    default_system = os.path.join(datadir, "galaxy", "bcbio_system.yaml")
    if not utils.file_exists(default_system):
        with open(default_system, "w") as out_handle:
            _, data = config_utils.get_dataarg(args)
            out = {"resources": tz.get_in(["config", "resources"], data, {})}
            yaml.safe_dump(out, out_handle, default_flow_style=False,
                           allow_unicode=False)


def get_output(target_file, pconfig):
    """Retrieve an output file from pack configuration.
    """
    if pconfig["type"] == "S3":
        keyname = "%s/%s" % (tz.get_in(["folders", "output"], pconfig),
                             os.path.basename(target_file))
        _transfer_s3(target_file, keyname, tz.get_in(["buckets", "run"],
                                                     pconfig))
        return target_file
    else:
        raise NotImplementedError("Unexpected pack information for "
                                  "fetchign output: %s" % pconfig)


def _transfer_s3(out_fname, keyname, bucket):
    if not os.path.exists(out_fname):
        utils.safe_makedir(os.path.dirname(out_fname))
        with file_transaction(out_fname) as tx_out_fname:
            subprocess.check_call(["gof3r", "get", "-p", tx_out_fname,
                                   "-k", keyname, "-b", bucket])


def _unpack_s3(bucket, args):
    """Create local directory in current directory with pulldowns from S3.
    """
    local_dir = utils.safe_makedir(os.path.join(os.getcwd(), bucket))
    remote_key = "s3://%s" % bucket

    def _get_s3(orig_fname, context, remap_dict):
        """Pull down s3 published data locally for processing.
        """
        if orig_fname.startswith(remote_key):
            if context[0] in ["reference", "genome_resources", "sam_ref"]:
                cur_dir = os.path.join(local_dir, "genomes")
            else:
                cur_dir = local_dir
            for fname in utils.file_plus_index(orig_fname):
                out_fname = fname.replace(remote_key, cur_dir)
                keyname = fname.replace(remote_key + "/", "")
                _transfer_s3(out_fname, keyname, bucket)
            return orig_fname.replace(remote_key, cur_dir)
        else:
            return orig_fname

    new_args = remap.walk_files(args, _get_s3, {remote_key: local_dir})
    return local_dir, new_args


def _remap_dict_shared(workdir, new_workdir, args):
    """Prepare a remap dictionary with directories we should potential
    copy files from.
    """
    ignore_keys = set(["algorithm"])
    out = {workdir: new_workdir}

    def _update_remap(fname, context, remap_dict):
        """Updated list of directories we should potentially be remapping in.
        """
        if not fname.startswith(tuple(out.keys())):
            if context and context[0] not in ignore_keys:
                dirname = os.path.normpath(os.path.dirname(fname))
                local_dir = utils.safe_makedir(
                    os.path.join(new_workdir, "external", str(len(out))))
                out[dirname] = local_dir

    remap.walk_files(args, _update_remap, {})
    return out


def _create_workdir_shared(workdir, args, parallel, tmpdir=None):
    """Create a work directory given inputs from the shared filesystem.

    If tmpdir is not None, we create a local working directory within the
    temporary space so IO and processing occurs there, remapping the input
    argument paths at needed.
    """
    if not tmpdir:
        return workdir, {}, args
    else:
        new_workdir = utils.safe_makedir(os.path.join(
            tmpdir, "bcbio-work-%s" % uuid.uuid1()))
        remap_dict = _remap_dict_shared(workdir, new_workdir, args)
        new_args = remap.walk_files(args, _remap_copy_file(parallel),
                                    remap_dict)
        return new_workdir, remap_dict, new_args


def is_required_resource(context, parallel):
    fresources = parallel.get("fresources")
    if not fresources:
        return True
    for fresource in fresources:
        if context[:len(fresource)] == fresource:
            return True
    return False


def _remap_copy_file(parallel):
    """Remap file names and copy into temporary directory as needed.

    Handles simultaneous transfer of associated indexes.
    """
    def _do(fname, context, orig_to_temp):
        new_fname = remap.remap_fname(fname, context, orig_to_temp)
        if os.path.isfile(fname):
            if is_required_resource(context, parallel):
                logger.info("YES: %s: %s" % (context, fname))
                utils.safe_makedir(os.path.dirname(new_fname))
                for ext in ["", ".idx", ".gbi", ".tbi", ".bai"]:
                    if os.path.exists(fname + ext):
                        if not os.path.exists(new_fname + ext):
                            shutil.copyfile(fname + ext, new_fname + ext)
            else:
                logger.info("NO: %s: %s" % (context, fname))
        elif os.path.isdir(fname):
            utils.safe_makedir(new_fname)
        return new_fname
    return _do


def _shared_finalizer(args, workdir, remap_dict, parallel):
    """Cleanup temporary working directory, copying missing files back
    to the shared workdir.
    """
    def _do(out):
        if remap_dict:
            new_remap_dict = {v: k for k, v in remap_dict.items()}
            new_out = (remap.walk_files(out, _remap_copy_file(parallel),
                                        new_remap_dict) if out else None)
            if os.path.exists(workdir):
                shutil.rmtree(workdir)
            return new_out
        else:
            return out
    return _do
