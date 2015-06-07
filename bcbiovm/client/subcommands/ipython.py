"""Run on a cluster using IPython parallel."""

import os

import yaml
from bcbio.distributed import clargs
from bcbio.pipeline import main

from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.docker import defaults as docker_defaults
from bcbiovm.docker import install as docker_install
from bcbiovm.docker import mounts as docker_mounts
from bcbiovm.docker import run as docker_run
from bcbiovm.ipython import batchprep
from bcbiovm.ship import pack


class IPython(base.BaseCommand):

    """Run on a cluster using IPython parallel."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "ipython",
            help="Run on a cluster using IPython parallel.")
        parser.add_argument(
            "sample_config",
            help="YAML file with details about samples to process.")
        parser.add_argument(
            "--fcdir",
            help="A directory of Illumina output or fastq files to process",
            type=lambda path: (os.path.abspath(os.path.expanduser(path))))
        parser.add_argument(
            "--systemconfig",
            help=("Global YAML configuration file specifying system details. "
                  "Defaults to installed bcbio_system.yaml."))
        parser.add_argument(
            "-n", "--numcores", type=int, default=1,
            help="Total cores to use for processing")
        parser.add_argument(
            "scheduler", help="Scheduler to use.",
            choices=["lsf", "sge", "torque", "slurm", "pbspro"])
        parser.add_argument(
            "queue", help="Scheduler queue to run jobs on.")
        parser.add_argument(
            "-r", "--resources",
            help=("Cluster specific resources specifications. "
                  "Can be specified multiple times.\n"
                  "Supports SGE and SLURM parameters."),
            default=[], action="append")
        parser.add_argument(
            "--timeout", default=15, type=int,
            help=("Number of minutes before cluster startup times out."
                  "Defaults to 15"))
        parser.add_argument(
            "--retries", default=0, type=int,
            help=("Number of retries of failed tasks during distributed "
                  "processing. Default 0 (no retries)"))
        parser.add_argument(
            "-t", "--tag", default="",
            help="Tag name to label jobs on the cluster")
        parser.add_argument(
            "--tmpdir",
            help="Path of local on-machine temporary directory to process in.")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        args = docker_defaults.update_check_args(
            self.args, "Could not run IPython parallel analysis.")
        args = docker_install.docker_image_arg(args)

        work_dir = os.getcwd()
        parallel = clargs.to_parallel(args, "bcbiovm.docker")
        parallel["wrapper"] = "runfn"

        with open(args.sample_config) as in_handle:
            ready_config, _ = docker_mounts.normalize_config(
                yaml.load(in_handle), args.fcdir)

        ready_config_file = os.path.join(
            work_dir, "%s-ready%s" %
            (os.path.splitext(os.path.basename(args.sample_config))))

        with open(ready_config_file, "w") as out_handle:
            yaml.safe_dump(ready_config, out_handle, default_flow_style=False,
                           allow_unicode=False)

        systemconfig = docker_run.local_system_config(
            args.systemconfig, args.datadir, work_dir)
        parallel["wrapper_args"] = [
            constant.DOCKER,
            {
                "sample_config": ready_config_file,
                "fcdir": args.fcdir,
                "pack": pack.shared_filesystem(work_dir, args.datadir,
                                               args.tmpdir),
                "systemconfig": systemconfig,
                "image": args.image
            }]

        # For testing, run on a local ipython cluster
        parallel["run_local"] = parallel.get("queue") == "localrun"

        main.run_main(work_dir,
                      run_info_yaml=ready_config_file,
                      config_file=systemconfig,
                      fc_dir=args.fcdir,
                      parallel=parallel)

        # Approach for running main function inside of docker.
        # Could be useful for architectures where we can spawn docker
        # jobs from docker.
        #
        # cmd_args = {
        #     "systemconfig": systemconfig,
        #     "image": args.image,
        #     "pack": cur_pack,
        #     "sample_config": args.sample_config,
        #     "fcdir": args.fcdir,
        #     "orig_systemconfig": args.systemconfig
        # }
        # main_args = [work_dir, ready_config_file, systemconfig,
        #              args.fcdir, parallel]
        # run.do_runfn("run_main", main_args, cmd_args, parallel,
        #              content.DOCKER)


class IPythonPrep(base.BaseCommand):

    """Prepare a batch script to run bcbio on a scheduler."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "ipythonprep",
            help="Prepare a batch script to run bcbio on a scheduler.")
        parser.add_argument(
            "sample_config",
            help="YAML file with details about samples to process.")
        parser.add_argument(
            "--fcdir",
            help="A directory of Illumina output or fastq files to process",
            type=lambda path: (os.path.abspath(os.path.expanduser(path))))
        parser.add_argument(
            "--systemconfig",
            help=("Global YAML configuration file specifying system details. "
                  "Defaults to installed bcbio_system.yaml."))
        parser.add_argument(
            "-n", "--numcores", type=int, default=1,
            help="Total cores to use for processing")
        parser.add_argument(
            "scheduler", help="Scheduler to use.",
            choices=["lsf", "sge", "torque", "slurm", "pbspro"])
        parser.add_argument(
            "queue", help="Scheduler queue to run jobs on.")
        parser.add_argument(
            "-r", "--resources",
            help=("Cluster specific resources specifications. "
                  "Can be specified multiple times.\n"
                  "Supports SGE and SLURM parameters."),
            default=[], action="append")
        parser.add_argument(
            "--timeout", default=15, type=int,
            help=("Number of minutes before cluster startup times out."
                  "Defaults to 15"))
        parser.add_argument(
            "--retries", default=0, type=int,
            help=("Number of retries of failed tasks during distributed "
                  "processing. Default 0 (no retries)"))
        parser.add_argument(
            "-t", "--tag", default="",
            help="Tag name to label jobs on the cluster")
        parser.add_argument(
            "--tmpdir",
            help="Path of local on-machine temporary directory to process in.")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        return batchprep.submit_script(self.args)
