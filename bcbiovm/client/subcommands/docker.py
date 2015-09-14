"""Utilities to help with develping using bcbion inside of docker."""
from __future__ import print_function

import os

import yaml

from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.common import utils
from bcbiovm.docker import devel as docker_devel
from bcbiovm.docker import install as docker_install
from bcbiovm.docker import manage as docker_manage
from bcbiovm.docker import run as docker_run
from bcbiovm.provider import factory as provider_factory

LOG = utils.get_logger(__name__)


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

    """Build docker image and export to the cloud provider."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "dockerbuild",
            help="Build docker image and export to the cloud provider.")
        parser.add_argument(
            "-c", "--container", default="bcbio_nextgen",
            help="The container name where to upload the gzipped "
                 "docker image to")
        parser.add_argument(
            "-t", "--buildtype", default="full", choices=["full", "code"],
            help=("Type of docker build to do. full is all code and third"
                  " party tools. code is only bcbio-nextgen code."))
        parser.add_argument(
            "-d", "--rundir", default="/tmp/bcbio-docker-build",
            help="Directory to run docker build in")
        parser.add_argument(
            "-p", "--provider", default=constant.DEFAULT_PROVIDER,
            help="The name of the cloud provider. (default=aws)")
        parser.add_argument(
            "--account_name", default=None,
            help="The storage account name. All access to Azure Storage"
                 " is done through a storage account.")
        parser.set_defaults(func=self.run)

    def process(self):
        """Run the command with the received information."""
        return docker_devel.run_docker_build(
            container=self.args.container,
            build_type=self.args.buildtype,
            run_directory=self.args.rundir,
            provider=self.args.provider,
            account_name=self.args.account_name,
        )


class BiodataUpload(base.DockerSubcommand):

    """Upload pre-prepared biological data to cache."""

    def __init__(self, *args, **kwargs):
        super(BiodataUpload, self).__init__(*args, **kwargs)
        self._need_prologue = True
        self._need_datadir = True

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
        parser.set_defaults(func=self.run)

    def process(self):
        """Manage preparation of biodata on a local machine, uploading
        to S3 in pieces."""
        return docker_devel.run_biodata_upload(self.args)


class SystemUpdate(base.DockerSubcommand):

    """Update bcbio system file with a given core and memory/core target."""

    def __init__(self, *args, **kwargs):
        super(SystemUpdate, self).__init__(*args, **kwargs)
        self._need_prologue = True
        self._need_datadir = True

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
        parser.set_defaults(func=self.run)

    def process(self):
        """Install python code from a bcbio-nextgen development tree
        inside of docker.
        """
        return docker_devel.run_setup_install(self.args)


class Run(base.DockerSubcommand):

    """Run an automated analysis on the local machine."""

    def __init__(self, *args, **kwargs):
        super(Run, self).__init__(*args, **kwargs)
        self._need_prologue = True
        self._need_datadir = True

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
        args = docker_install.docker_image_arg(self.args)
        docker_run.do_analysis(args, constant.DOCKER)


class RunFunction(base.DockerSubcommand):

    """Run a specific bcbio-nextgen function with provided arguments."""

    def __init__(self, *args, **kwargs):
        super(RunFunction, self).__init__(*args, **kwargs)
        self._need_prologue = True
        self._need_datadir = True

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
        args = docker_install.docker_image_arg(self.args)

        # FIXME(alexandrucoman): Taking cloud provider into consideration
        shipping_config = provider_factory.get_ship_config("S3", raw=False)
        ship = provider_factory.get_ship("S3")

        with open(args.parallel) as in_handle:
            parallel = yaml.safe_load(in_handle)

        with open(args.runargs) as in_handle:
            runargs = yaml.safe_load(in_handle)

        cmd_args = {
            "systemconfig": args.systemconfig,
            "image": args.image,
            "pack": shipping_config(parallel["pack"]),
        }
        out = docker_run.do_runfn(args.fn_name, runargs, cmd_args,
                                  parallel, constant.DOCKER)
        out_file = "%s-out%s" % os.path.splitext(args.runargs)
        with open(out_file, "w") as out_handle:
            yaml.safe_dump(out, out_handle, default_flow_style=False,
                           allow_unicode=False)

        ship.pack.send_output(shipping_config(parallel["pack"]), out_file)


class Install(base.DockerSubcommand):

    """Install bcbio-nextgen docker container and data."""

    def __init__(self, *args, **kwargs):
        super(Install, self).__init__(*args, **kwargs)
        self._need_prologue = True

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        _install_or_upgrade(main_parser=self._main_parser,
                            callback=self.run,
                            install=True)

    def prologue(self):
        """Executed once before the arguments parsing."""
        self._need_datadir = self.args.install_data
        super(Install, self).epilogue()

    def process(self):
        """Run the command with the received information."""
        docker_install.full(self.args, constant.DOCKER)


class Upgrade(base.DockerSubcommand):

    """Upgrade bcbio-nextgen docker container and data."""

    def __init__(self, *args, **kwargs):
        super(Upgrade, self).__init__(*args, **kwargs)
        self._need_prologue = True

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        _install_or_upgrade(main_parser=self._main_parser,
                            callback=self.run,
                            install=False)

    def prologue(self):
        """Executed once before the arguments parsing."""
        self._need_datadir = self.args.install_data
        super(Upgrade, self).epilogue()

    def process(self):
        """Run the command with the received information."""
        docker_install.full(self.args, constant.DOCKER)


class Server(base.DockerSubcommand):

    """Persistent REST server receiving requests via the specified port."""

    def __init__(self, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)
        self._need_prologue = True
        self._need_datadir = True

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
        args = docker_install.docker_image_arg(self.args)
        ports = ["%s:%s" % (args.port, constant.DOCKER["port"])]
        print("Running server on port %s. Press ctrl-c to exit." % args.port)
        docker_manage.run_bcbio_cmd(
            image=args.image, mounts=[], ports=ports,
            bcbio_nextgen_args=["server", "--port",
                                str(constant.DOCKER["port"])],
        )


class SaveConfig(base.DockerSubcommand):

    """Save standard configuration variables for current user."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._main_parser.add_parser(
            "saveconfig",
            help="Save standard configuration variables for current user. "
                 "Avoids need to specify on the command line in future runs.")
        parser.set_defaults(func=self.run)

    def process(self):
        """Save user specific defaults to a yaml configuration file."""
        new_config = self._get_defaults()
        for config, value in self._defaults:
            args_value = getattr(self.args, config, None)
            if args_value and args_value != value:
                new_config[config] = args_value

        if new_config:
            config_file = self._get_config_file(just_filename=True)
            with open(config_file, "w") as config_handle:
                yaml.dump(new_config, config_handle, default_flow_style=False,
                          allow_unicode=False)
