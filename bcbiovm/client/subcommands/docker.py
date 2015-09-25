"""Utilities to help with develping using bcbion inside of docker."""
from __future__ import print_function

import os

import yaml

from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.common import exception
from bcbiovm.common import utils
from bcbiovm.docker import devel as docker_devel
from bcbiovm.docker import manage as docker_manage
from bcbiovm.docker import run as docker_run
from bcbiovm.provider import factory as provider_factory

LOG = utils.get_logger(__name__)

# Because the tool namespace is injected in the parent
# command, pylint thinks that the arguments from the tool's
# namespace did not exist.
# In order to avoid `no-memeber` error we will disable this
# error.

# pylint: disable=no-member


class _Action(base.Command):

    """Install or upgrade the bcbio-nextgen docker container and data."""

    _NO_GENOMES = ("Data not installed, no genomes provided with "
                   "`--genomes` flag")
    _NO_ALIGNERS = ("Data not installed, no aligners provided with "
                    "`--aligners` flag")

    def __init__(self, install, *args, **kwargs):
        super(_Action, self).__init__(*args, **kwargs)
        self._action = "Install" if install else "Upgrade"
        self._updates = []

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            self._action.lower(),
            help="{action} bcbio-nextgen docker container "
                 "and data.".format(action=self._action))
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
        parser.set_defaults(work=self.run)

    def _check_install_data(self):
        """Check if the command line contains all the required
        information in order to install the data."""
        if not self.args.install_data:
            return

        if len(self.args.genomes) == 0:
            raise exception.BCBioException(self._NO_GENOMES)

        if len(self.args.aligners) == 0:
            raise exception.BCBioException(self._NO_ALIGNERS)

    def _get_command_line(self):
        """Prepare the command line for upgrade the biodata."""
        arguments = ["upgrade"]
        if self.args.install_data:
            arguments.append("--data")
            for genome in self.args.genomes:
                arguments.extend(["--genomes", genome])
            for aligner in self.args.aligners:
                arguments.extend(["--aligners", aligner])
        return arguments

    def prologue(self):
        """Executed once before the arguments parsing."""
        # Add user configured defaults to supplied command line arguments.
        self.defaults.add()
        # Retrieve supported remote inputs specified on the command line.
        self.defaults.retrieve()
        if self.args.install_data:
            # Check if the datadir exists
            self.defaults.datadir("bcbio-nextgen not installed or upgrade.")

        # Add previously saved installation defaults to command line
        # arguments.
        self.install.defaults()

    def work(self):
        """Run the command with the received information."""
        mounts = self.common.prepare_system(self.args.datadir,
                                            constant.DOCKER["biodata_dir"])

        if self.args.wrapper:
            self._updates.append("wrapper scripts")
            self.common.upgrade_bcbio_vm()

        if self.args.install_tools:
            self._updates.append("bcbio-nextgen code and third party tools")
            self.common.pull_image(constant.DOCKER)
            # Ensure external galaxy configuration in sync when
            # doing tool upgrade
            docker_manage.run_bcbio_cmd(self.args.image, mounts, ["upgrade"])

        if self.args.install_data:
            self._updates.append("biological data")
            self.common.check_image()
            docker_manage.run_bcbio_cmd(self.args.image, mounts,
                                        self._get_command_line())

        self.install.save_defaults()

    def epilogue(self):
        if self._updates:
            LOG.info("bcbio-nextgen-vm updated with latest %(updates)s",
                     {"updates": " and ".join(self._updates)})
        else:
            LOG.warning("No update targets specified, need '--wrapper', "
                        "'--tools' or '--data'")
            LOG.info("See 'bcbio_vm.py upgrade -h' for more details.")


class Install(_Action):

    """Install bcbio-nextgen docker container and data."""

    def __init__(self, *args, **kwargs):
        super(Install, self).__init__(install=True, *args, **kwargs)


class Upgrade(_Action):

    """Upgrade bcbio-nextgen docker container and data."""

    def __init__(self, *args, **kwargs):
        super(Upgrade, self).__init__(install=False, *args, **kwargs)


class Build(base.Command):

    """Build docker image and export to the cloud provider."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
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
        parser.set_defaults(work=self.run)

    def work(self):
        """Run the command with the received information."""
        return docker_devel.run_docker_build(
            container=self.args.container,
            build_type=self.args.buildtype,
            run_directory=self.args.rundir,
            provider=self.args.provider,
            account_name=self.args.account_name,
        )


class BiodataUpload(base.Command):

    """Upload pre-prepared biological data to cache."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
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
        parser.set_defaults(work=self.run)

    def prologue(self):
        """Executed once before the arguments parsing."""
        # Add user configured defaults to supplied command line arguments.
        self.defaults.add()
        # Retrieve supported remote inputs specified on the command line.
        self.defaults.retrieve()
        # Check if the datadir exists if it is required.
        self.defaults.datadir("Biodata not uploaded.")
        # Add all the missing arguments related to docker image.
        self.install.image_defaults()
        # Check if the docker image exists.
        self.common.check_image()

    def work(self):
        """Manage preparation of biodata on a local machine, uploading
        to S3 in pieces."""
        mounts = self.common.prepare_system(self.args.datadir,
                                            constant.DOCKER["biodata_dir"])
        return docker_devel.run_biodata_upload(self.args, mounts)


class SystemUpdate(base.Command):

    """Update bcbio system file with a given core and memory/core target."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "system",
            help=("Update bcbio system file with a given core "
                  "and memory/core target"))
        parser.add_argument(
            "cores",
            help="Target cores to use for multi-core processes")
        parser.add_argument(
            "memory",
            help="Target memory per core, in Mb (1000 = 1Gb)")
        parser.set_defaults(work=self.run)

    def prologue(self):
        """Executed once before the arguments parsing."""
        # Add user configured defaults to supplied command line arguments.
        self.defaults.add()
        # Retrieve supported remote inputs specified on the command line.
        self.defaults.retrieve()
        # Check if the datadir exists if it is required.
        self.defaults.datadir("Could not do upgrade of bcbio_system.yaml.")

    def work(self):
        """Update bcbio_system.yaml file with a given target of cores
        and memory.
        """
        return docker_devel.run_system_update(self.args)


class SetupInstall(base.Command):

    """Run a python setup.py install inside of the current directory."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "setup_install",
            help=("Run a python setup.py install inside of "
                  "the current directory"))
        parser.add_argument(
            "-i", "--image", help="Image name to write updates to",
            default=constant.DOCKER_DEFAULT_IMAGE)
        parser.set_defaults(work=self.run)

    def work(self):
        """Install python code from a bcbio-nextgen development tree
        inside of docker.
        """
        return docker_devel.run_setup_install(self.args)


class Run(base.Command):

    """Run an automated analysis on the local machine."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
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
        parser.set_defaults(work=self.run)

    def prologue(self):
        """Executed once before the arguments parsing."""
        # Add user configured defaults to supplied command line arguments.
        self.defaults.add()
        # Retrieve supported remote inputs specified on the command line.
        self.defaults.retrieve()
        # Check if the datadir exists if it is required.
        self.defaults.datadir("Could not run analysis.")

    def work(self):
        """Run the command with the received information."""
        mounts = self.common.prepare_system(self.args.datadir,
                                            constant.DOCKER["biodata_dir"])
        docker_run.do_analysis(self.args, constant.DOCKER, mounts)


class RunFunction(base.Command):

    """Run a specific bcbio-nextgen function with provided arguments."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
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
        parser.set_defaults(work=self.run)

    def prologue(self):
        """Executed once before the arguments parsing."""
        # Add user configured defaults to supplied command line arguments.
        self.defaults.add()
        # Retrieve supported remote inputs specified on the command line.
        self.defaults.retrieve()
        # Check if the datadir exists if it is required.
        self.defaults.datadir("Could not run bcbio-nextgen function.")

    def work(self):
        """Run the command with the received information."""
        # FIXME(alexandrucoman): Taking cloud provider into consideration
        shipping_config = provider_factory.get_ship_config("S3", raw=False)
        ship = provider_factory.get_ship("S3")

        with open(self.args.parallel) as in_handle:
            parallel = yaml.safe_load(in_handle)

        with open(self.args.runargs) as in_handle:
            runargs = yaml.safe_load(in_handle)

        cmd_args = {
            "systemconfig": self.args.systemconfig,
            "image": self.args.image,
            "pack": shipping_config(parallel["pack"]),
        }
        out = docker_run.do_runfn(self.args.fn_name, runargs, cmd_args,
                                  parallel, constant.DOCKER)
        out_file = "%s-out%s" % os.path.splitext(self.args.runargs)
        with open(out_file, "w") as out_handle:
            yaml.safe_dump(out, out_handle, default_flow_style=False,
                           allow_unicode=False)

        ship.pack.send_output(shipping_config(parallel["pack"]), out_file)


class Server(base.Command):

    """Persistent REST server receiving requests via the specified port."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "server",
            help=("Persistent REST server receiving requests "
                  "via the specified port."))
        parser.add_argument(
            "--port", default=8085,
            help="External port to connect to docker image.")
        parser.set_defaults(work=self.run)

    def prologue(self):
        """Executed once before the arguments parsing."""
        # Add user configured defaults to supplied command line arguments.
        self.defaults.add()
        # Retrieve supported remote inputs specified on the command line.
        self.defaults.retrieve()
        # Check if the datadir exists if it is required.
        self.defaults.datadir("Could not run server.")

    def work(self):
        """Run the command with the received information."""
        ports = ["%s:%s" % (self.args.port, constant.DOCKER["port"])]
        print("Running server on port %s. Press ctrl-c to exit." %
              self.args.port)
        docker_manage.run_bcbio_cmd(
            image=self.args.image, mounts=[], ports=ports,
            bcbio_nextgen_args=["server", "--port",
                                str(constant.DOCKER["port"])],
        )


class SaveConfig(base.Command):

    """Save standard configuration variables for current user."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "saveconfig",
            help="Save standard configuration variables for current user. "
                 "Avoids need to specify on the command line in future runs.")
        parser.set_defaults(work=self.run)

    def work(self):
        """Save user specific defaults to a yaml configuration file."""
        # defaults.save is alias for tools.DockerDefaults.save_defaults
        self.defaults.save()
