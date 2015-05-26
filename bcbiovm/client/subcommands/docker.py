"""Utilities to help with develping using bcbion inside of docker."""
import os

import yaml

from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.docker import defaults as docker_defaults
from bcbiovm.docker import devel as docker_devel
from bcbiovm.docker import install as docker_install
from bcbiovm.docker import run as docker_run
from bcbiovm.ship import pack as ship_pack


class Build(base.BaseCommand):

    """Build docker image and export to S3."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "dockerbuild",
            help="Build docker image and export to S3")
        parser.add_argument(
            "-b", "--bucket", default="bcbio_nextgen",
            help="S3 bucket to upload the gzipped docker image to")
        parser.add_argument(
            "-t", "--buildtype", default="full", choices=["full", "code"],
            help=("Type of docker build to do. full is all code and third"
                  " party tools. code is only bcbio-nextgen code."))
        parser.add_argument(
            "-d", "--rundir", default="/tmp/bcbio-docker-build",
            help="Directory to run docker build in")
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        return docker_devel.run_docker_build(self.args)


class BiodataUpload(base.BaseCommand):

    """Upload pre-prepared biological data to cache."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "biodata",
            help="Upload pre-prepared biological data to cache")
        parser.add_argument(
            "--prepped",
            help=("Start with an existing set of cached data to "
                  "output directory."))
        parser.add_argument(
            "--genomes", help="Genomes to download",
            action="append", default=[],
            choices=["GRCh37", "hg19", "mm10", "mm9", "rn5", "canFam3", "dm3",
                     "Zv9", "phix", "sacCer3", "xenTro3", "TAIR10",
                     "WBcel235"])
        parser.add_argument(
            "--aligners", help="Aligner indexes to download",
            action="append", default=[],
            choices=["bowtie", "bowtie2", "bwa", "novoalign", "star", "ucsc"])
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.set_defaults(func=self.run)

    def process(self):
        """Manage preparation of biodata on a local machine, uploading
        to S3 in pieces."""
        return docker_devel.run_biodata_upload(self.args)


class SystemUpdate(base.BaseCommand):

    """Update bcbio system file with a given core and memory/core target."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "system",
            help=("Update bcbio system file with a given core "
                  "and memory/core target"))
        parser.add_argument(
            "cores",
            help="Target cores to use for multi-core processes")
        parser.add_argument(
            "memory",
            help="Target memory per core, in Mb (1000 = 1Gb)")
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.set_defaults(func=self.run)

    def process(self):
        """Update bcbio_system.yaml file with a given target of cores
        and memory.
        """
        return docker_devel.run_system_update(self.args)


class SetupInstall(base.BaseCommand):

    """Run a python setup.py install inside of the current directory."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "setup_install",
            help=("Run a python setup.py install inside of "
                  "the current directory"))
        parser.add_argument(
            "-i", "--image", help="Image name to write updates to",
            default=constant.DOCKER_DEFAULT_IMAGE)
        parser.add_argument(
            "-q", "--quiet", dest="verbose", action="store_false",
            default=True, help="Quiet output when running Ansible playbooks")
        parser.set_defaults(func=self.run)

    def process(self):
        """Install python code from a bcbio-nextgen development tree
        inside of docker.
        """
        return docker_devel.run_setup_install(self.args)


class RunFunction(base.BaseCommand):

    """Run a specific bcbio-nextgen function with provided arguments."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "runfn",
            help=("Run a specific bcbio-nextgen function with provided"
                  " arguments"))
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
            "fn_name",
            help="Name of the function to run")
        parser.add_argument(
            "parallel",
            help="JSON/YAML file describing the parallel environment")
        parser.add_argument(
            "runargs",
            help="JSON/YAML file with arguments to the function")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        args = docker_defaults.update_check_args(
            self.args, "Could not run bcbio-nextgen function.")
        args = docker_install.docker_image_arg(args)

        with open(args.parallel) as in_handle:
            parallel = yaml.safe_load(in_handle)

        with open(args.runargs) as in_handle:
            runargs = yaml.safe_load(in_handle)

        cmd_args = {"systemconfig": args.systemconfig,
                    "image": args.image, "pack": parallel["pack"]}
        out = docker_run.do_runfn(args.fn_name, runargs, cmd_args,
                                  parallel, constant.DOCKER)
        out_file = "%s-out%s" % os.path.splitext(args.runargs)
        with open(out_file, "w") as out_handle:
            yaml.safe_dump(out, out_handle, default_flow_style=False,
                           allow_unicode=False)
        ship_pack.send_output(parallel["pack"], out_file)
