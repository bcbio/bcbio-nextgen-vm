"""Reconstitute an analysis in a temporary directory on the current machine.

Handles copying or linking files into a work directory, running an analysis,
then handing off outputs to ship back to subsequent processing steps.
"""
# pylint: disable=no-self-use

import abc
import os
import shutil
import uuid

import six
import toolz
import yaml

from bcbio.distributed.transaction import file_transaction
from bcbio.log import logger
from bcbio.pipeline import config_utils
from bcbio import utils

from bcbiovm.common import utils as common_utils
from bcbiovm.docker import remap
from bcbiovm.ship import pack as ship_n_pack


@six.add_metaclass(abc.ABCMeta)
class Reconstitute(object):

    """Reconstitute an analysis in a temporary directory
    on the current machine.

    Handles copying or linking files into a work directory,
    running an analysis, then handing off outputs to ship
    back to subsequent processing steps.
    """

    @staticmethod
    def is_required_resource(context, parallel):
        fresources = parallel.get("fresources")
        if not fresources:
            return True
        for fresource in fresources:
            if context[:len(fresource)] == fresource:
                return True
        return False

    @staticmethod
    def prep_systemconfig(datadir, args):
        """Prepare system configuration files on bare systems
        if not present.
        """
        default_system = os.path.join(datadir, "galaxy", "bcbio_system.yaml")
        if utils.file_exists(default_system):
            return

        with open(default_system, "w") as out_handle:
            _, data = config_utils.get_dataarg(args)
            output = {"resources": toolz.get_in(["config", "resources"],
                                                data, {})}
            yaml.safe_dump(output, out_handle, default_flow_style=False,
                           allow_unicode=False)

    def prepare_datadir(self, pack, args):
        """Prepare the biodata directory."""
        if "datadir" in pack:
            return pack["datadir"], args

        raise ValueError("Cannot handle biodata directory "
                         "preparation type: %s" % pack)

    @abc.abstractmethod
    def get_output(self, target_file, pconfig):
        """Retrieve an output file from pack configuration."""
        pass

    @abc.abstractmethod
    def prepare_workdir(self, pack, parallel, args):
        """Unpack necessary files and directories into a temporary structure
        for processing.
        """
        pass


class ReconstituteShared(Reconstitute):

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

        remap.walk_files(args, _callback, {})
        return output

    def _remap_copy_file(self, parallel):
        """Remap file names and copy into temporary directory as needed.

        Handles simultaneous transfer of associated indexes.
        """
        def _callback(fname, context, orig_to_temp):
            """Callback for bcbio.docker.remap.walk_files."""
            new_fname = remap.remap_fname(fname, context, orig_to_temp)
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

        callback = self._remap_copy_file(parallel)
        new_workdir = os.path.join(tmpdir, "bcbio-work-%s" % uuid.uuid1())
        utils.safe_makedir(new_workdir)

        remap_dict = self._remap_dict(workdir, new_workdir, args)
        new_args = remap.walk_files(args, callback, remap_dict)
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
                new_output = remap.walk_files(output, callback, new_remap_dict)

            if os.path.exists(workdir):
                shutil.rmtree(workdir)

            return new_output

        return _callback

    def prepare_workdir(self, pack, parallel, args):
        """Unpack necessary files and directories into a temporary structure
        for processing.
        """
        workdir, remap_dict, new_args = self._create_workdir(
            pack["workdir"], args, parallel, pack["tmpdir"])
        callback = self._shared_finalizer(workdir, remap_dict, parallel)
        return (workdir, new_args, callback)

    def get_output(self, target_file, pconfig):
        """Retrieve an output file from pack configuration."""
        raise NotImplementedError("Unexpected pack information for "
                                  "fetchign output: %s" % pconfig)


class ReconstituteS3(Reconstitute):

    @staticmethod
    def _download(out_fname, keyname, bucket):
        """Download file from Amazon S3."""
        if os.path.exists(out_fname):
            return

        utils.safe_makedir(os.path.dirname(out_fname))
        with file_transaction(out_fname) as tx_out_fname:
            common_utils.execute(
                ["gof3r", "get", "-p", tx_out_fname,
                 "-k", keyname, "-b", bucket],
                check_exit_code=True)

    def _unpack(self, bucket, args):
        """Create local directory in current directory with pulldowns
        from S3.
        """
        local_dir = utils.safe_makedir(os.path.join(os.getcwd(), bucket))
        remote_key = "s3://%s" % bucket

        def _callback(orig_fname, context, remap_dict):
            """Pull down s3 published data locally for processing."""
            # pylint: disable=unused-argument
            if not orig_fname.startswith(remote_key):
                return orig_fname

            if context[0] in ["reference", "genome_resources", "sam_ref"]:
                cur_dir = os.path.join(local_dir, "genomes")
            else:
                cur_dir = local_dir

            for fname in utils.file_plus_index(orig_fname):
                out_fname = fname.replace(remote_key, cur_dir)
                keyname = fname.replace(remote_key + "/", "")
                self._download(out_fname, keyname, bucket)
            return orig_fname.replace(remote_key, cur_dir)

        new_args = remap.walk_files(args, _callback, {remote_key: local_dir})
        return local_dir, new_args

    def prepare_workdir(self, pack, parallel, args):
        """Unpack necessary files and directories into a temporary structure
        for processing.
        """

        s3pack = ship_n_pack.S3Pack()
        workdir, new_args = self._unpack(pack["buckets"]["run"], args)
        datai, data = config_utils.get_dataarg(new_args)
        if "dirs" not in data:
            data["dirs"] = {}
        data["dirs"]["work"] = workdir
        new_args[datai] = data
        return workdir, new_args, s3pack.send_run_integrated(pack)

    def get_output(self, target_file, pconfig):
        """Retrieve an output file from pack configuration."""
        keyname = "%s/%s" % (toolz.get_in(["folders", "output"], pconfig),
                             os.path.basename(target_file))
        self._download(target_file, keyname,
                       toolz.get_in(["buckets", "run"], pconfig))
        return target_file

    def prepare_datadir(self, pack, args):
        """Prepare the biodata directory."""
        if pack["type"] == "S3":
            return self._unpack(pack["buckets"]["biodata"], args)

        return super(ReconstituteS3, self).prepare_datadir(pack, args)
