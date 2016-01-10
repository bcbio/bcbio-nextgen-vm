"""Prepare a running process to execute remotely and reconstitute
an analysis in a temporary directory on the current machine.
"""

import os
import shutil
import uuid

from bcbio import utils
from bcbio.log import logger

from bcbiovm.common import objects
from bcbiovm.container.docker import remap as docker_remap
from bcbiovm.provider import base


def get_shipping_config(workdir, datadir, tmpdir=None):
    """Enable running processing within an optional temporary directory.

    :param workdir: is assumed to be available on a shared filesystem,
    so we don't require any work to prepare.
    """
    config = {
        "type": "shared",
        "workdir": workdir,
        "tmpdir": tmpdir,
        "datadir": datadir
    }
    return config


def shipping_config(config):
    """Create a ShippingConfig object with the received information."""
    shared_config = objects.ShippingConfig(config)
    return shared_config


class ReconstituteShared(base.Reconstitute):

    """Reconstitute an analysis in a temporary directory on the
    current machine.

    Handles copying or linking files into a work directory,
    running an analysis, then handing off outputs to ship
    back to subsequent processing steps.
    """

    _EXTENSIONS = ("", ".idx", ".gbi", ".tbi", ".bai")

    @staticmethod
    def _remap_dict(workdir, new_workdir, args):
        """Prepare a remap dictionary with directories we should potential
        copy files from.
        """
        ignore_keys = set(["algorithm"])
        output = {workdir: new_workdir}

        def _callback(fname, context, remap_dict):
            """Updated list of directories we should potentially be
            remapping in.
            """
            # pylint: disable=unused-argument
            if fname.startswith(tuple(output.keys())):
                return
            if not context or context[0] in ignore_keys:
                return

            dirname = os.path.normpath(os.path.dirname(fname))
            local_dir = os.path.join(new_workdir, "external",
                                     str(len(output)))
            utils.safe_makedir(local_dir)
            output[dirname] = local_dir

        docker_remap.walk_files(args, _callback, {})
        return output

    def _remap_copy_file(self, parallel):
        """Remap file names and copy into temporary directory as needed.

        Handles simultaneous transfer of associated indexes.
        """
        def _callback(fname, context, orig_to_temp):
            """Callback for bcbio.docker.remap.walk_files."""
            new_fname = docker_remap.remap_fname(fname, context, orig_to_temp)
            if os.path.isfile(fname):
                if self.is_required_resource(context, parallel):
                    logger.info("YES: %s: %s" % (context, fname))
                    utils.safe_makedir(os.path.dirname(new_fname))
                    for ext in self._EXTENSIONS:
                        if not os.path.exists(fname + ext):
                            continue
                        if not os.path.exists(new_fname + ext):
                            shutil.copyfile(fname + ext, new_fname + ext)
                else:
                    logger.info("NO: %s: %s" % (context, fname))
            elif os.path.isdir(fname):
                utils.safe_makedir(new_fname)
            return new_fname
        return _callback

    def _create_workdir(self, workdir, args, parallel, tmpdir=None):
        """Create a work directory given inputs from the shared filesystem.

        If tmpdir is not None, we create a local working directory within the
        temporary space so IO and processing occurs there, remapping the input
        argument paths at needed.
        """
        if not tmpdir:
            return (workdir, {}, args)

        new_workdir = os.path.join(tmpdir, "bcbio-work-%s" % uuid.uuid1())
        utils.safe_makedir(new_workdir)

        remap_dict = self._remap_dict(workdir, new_workdir, args)
        callback = self._remap_copy_file(parallel)
        new_args = docker_remap.walk_files(args, callback, remap_dict)

        return (new_workdir, remap_dict, new_args)

    def _shared_finalizer(self, workdir, remap_dict, parallel):
        """Cleanup temporary working directory, copying missing files back
        to the shared workdir.
        """
        def _callback(output):
            """Callback for bcbio.docker.remap.walk_files."""
            if not remap_dict:
                return output

            new_output = None
            new_remap_dict = {value: key for key, value in remap_dict.items()}

            if output:
                callback = self._remap_copy_file(parallel)
                new_output = docker_remap.walk_files(output, callback,
                                                     new_remap_dict)

            if os.path.exists(workdir):
                shutil.rmtree(workdir)

            return new_output

        return _callback

    def prepare_workdir(self, pack, parallel, args):
        """Unpack necessary files and directories into a temporary structure
        for processing.
        """
        workdir, remap_dict, new_args = self._create_workdir(
            pack.workdir, args, parallel, pack.tmpdir)

        callback = self._shared_finalizer(workdir, remap_dict, parallel)
        return (workdir, new_args, callback)

    def get_output(self, target_file, pconfig):
        """Retrieve an output file from pack configuration."""
        raise NotImplementedError("Unexpected pack information for "
                                  "fetchign output: %s" % pconfig)
