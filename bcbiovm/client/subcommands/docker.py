"""Utilities to help with develping using bcbion inside of docker."""
from __future__ import print_function
import os

import yaml

from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.docker import defaults as docker_defaults
from bcbiovm.docker import devel as docker_devel
from bcbiovm.docker import install as docker_install
from bcbiovm.docker import manage as docker_manage
from bcbiovm.docker import run as docker_run
from bcbiovm.provider import factory as provider_factory


def _install_or_upgrade(main_parser, callback, install=True):
    """Add to the received parser the install or the upgrade
    command.
    """
    action = "Install" if install else "Upgrade"
    parser = main_parser.add_parser(
        action.lower(),
        help="{action} bcbio-nextgen docker container and data.".format(
            action=action))
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
        "--data", help="Install or upgrade data dependencies",
        dest="install_data", action="store_true", default=False)
    parser.add_argument(
        "--tools", help="Install or upgrade tool dependencies",
        dest="install_tools", action="store_true", default=False)
    parser.add_argument(
        "--wrapper", help="Update wrapper bcbio-nextgen-vm code",
        action="store_true", default=False)
    parser.add_argument(
        "--image", default=None,
        help=("Docker image name to use, could point to compatible "
              "pre-installed image."))
    parser.set_defaults(func=callback)


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


class Run(base.BaseCommand):

    """Run an automated analysis on the local machine."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "run",
            help="Run an automated analysis on the local machine.")
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
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        args = docker_defaults.update_check_args(self.args,
                                                 "Could not run analysis.")
        args = docker_install.docker_image_arg(args)
        docker_run.do_analysis(args, constant.DOCKER)


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

        # FIXME(alexandrucoman): Taking cloud provider into consideration
        shiping_config = provider_factory.get_ship_config("S3", raw=False)
        ship = provider_factory.get_ship("S3")

        with open(args.parallel) as in_handle:
            parallel = yaml.safe_load(in_handle)

        with open(args.runargs) as in_handle:
            runargs = yaml.safe_load(in_handle)

        cmd_args = {"systemconfig": args.systemconfig,
                    "image": args.image,
                    "pack": parallel["pack"]}
        out = docker_run.do_runfn(args.fn_name, runargs, cmd_args,
                                  parallel, constant.DOCKER)
        out_file = "%s-out%s" % os.path.splitext(args.runargs)
        with open(out_file, "w") as out_handle:
            yaml.safe_dump(out, out_handle, default_flow_style=False,
                           allow_unicode=False)

        ship.pack.send_output(shiping_config(parallel["pack"]), out_file)


class Install(base.BaseCommand):

    """Install bcbio-nextgen docker container and data."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        _install_or_upgrade(main_parser=self._main_parser,
                            callback=self.run,
                            install=True)

    def process(self):
        """Run the command with the received information."""
        args = docker_defaults.update_check_args(
            self.args, "bcbio-nextgen not upgraded.",
            need_datadir=self.args.install_data)
        docker_install.full(args, constant.DOCKER)


class Upgrade(base.BaseCommand):

    """Upgrade bcbio-nextgen docker container and data."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        _install_or_upgrade(main_parser=self._main_parser,
                            callback=self.run,
                            install=False)

    def process(self):
        """Run the command with the received information."""
        args = docker_defaults.update_check_args(
            self.args, "bcbio-nextgen not upgraded.",
            need_datadir=self.args.install_data)
        docker_install.full(args, constant.DOCKER)


class Server(base.BaseCommand):

    """Persistent REST server receiving requests via the specified port."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "server",
            help=("Persistent REST server receiving requests "
                  "via the specified port."))
        parser.add_argument(
            "--port", default=8085,
            help="External port to connect to docker image.")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        args = docker_defaults.update_check_args(self.args,
                                                 "Could not run server.")
        args = docker_install.docker_image_arg(args)
        ports = ["%s:%s" % (args.port, constant.DOCKER["port"])]
        print("Running server on port %s. Press ctrl-c to exit." % args.port)
        docker_manage.run_bcbio_cmd(
            image=args.image, mounts=[], ports=ports,
            bcbio_nextgen_args=["server", "--port",
                                str(constant.DOCKER["port"])],
        )


class SaveConfig(base.BaseCommand):

    """Save standard configuration variables for current user."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "saveconfig",
            help="Save standard configuration variables for current user. "
                 "Avoids need to specify on the command line in future runs.")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        return docker_defaults.save(self.args)
