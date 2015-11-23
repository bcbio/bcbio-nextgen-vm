"""Utilities to help with develping using bcbion inside of docker."""
from __future__ import print_function

import abc
import os

import six

from bcbiovm import config as bcbio_config
from bcbiovm import log as logging
from bcbiovm.client import base
from bcbiovm.common import constant
from bcbiovm.common import exception
from bcbiovm.common import utils as common_utils
from bcbiovm.container.docker import docker_container
from bcbiovm.container.docker import common as docker_common

LOG = logging.get_logger(__name__)


@six.add_metaclass(abc.ABCMeta)
class Build(base.Command):

    """Build docker image and export to the cloud provider."""

    @abc.abstractmethod
    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        pass

    def work(self):
        """Run the command with the received information."""
        container = docker_container.Docker()
        image_path = container.build_image(cwd=self.args.rundir,
                                           full=self.args.buildtype == "full")

        if image_path and self.args.upload:
            LOG.info("Uploading docker image %s ...", image_path)
            container.upload_image(path=image_path,
                                   container=self.args.container,
                                   storage=self.args.storage,
                                   context=self.args.context)

            LOG.info("The docker image was successfully uploaded ! "
                     "Image URL: https://%s.blob.core.windows.net/%s/%s",
                     self.args.storage_account, self.args.container,
                     os.path.basename(image_path))


@six.add_metaclass(abc.ABCMeta)
class BiodataUpload(base.Command):

    """Upload pre-prepared biological data to cache."""

    @abc.abstractmethod
    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        pass

    def prologue(self):
        """Executed once before the arguments parsing."""
        if not self.args.prepped:
            return

        # Add user configured defaults to supplied command line arguments.
        self.defaults.add_defaults()
        # Retrieve supported remote inputs specified on the command line.
        self.defaults.retrieve()
        # Check if the datadir exists if it is required.
        self.defaults.check_datadir("Biodata not uploaded.")
        # Add all the missing arguments related to docker image.
        self.install.image_defaults()

    def work(self):
        """Manage preparation of biodata on a local machine, uploading
        to a storage manager in pieces."""
        container = docker_container.Docker()

        if self.args.prepped:
            return container.prepare_genomes(genomes=self.args.genomes,
                                             aligners=self.args.aligners,
                                             output=self.args.prepped)

        # Check if the docker image exists.
        container.check_image(self.args.image)
        return container.upload_biodata(genomes=self.args.genomes,
                                        aligners=self.args.aligners,
                                        image=self.args.image,
                                        datadir=self.args.datadir,
                                        provider=self.args.provider,
                                        context=self.args.context)


class _Action(base.Command):

    """Install or upgrade the bcbio-nextgen docker container and data."""

    _NO_GENOMES = ("Data not installed, no genomes provided with "
                   "`--genomes` flag")
    _NO_ALIGNERS = ("Data not installed, no aligners provided with "
                    "`--aligners` flag")

    def __init__(self, install, parent, parser):
        self._action = "Install" if install else "Upgrade"
        self._updates = []
        super(_Action, self).__init__(parent, parser)

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            self._action.lower(),
            help="{action} bcbio-nextgen docker container "
                 "and data.".format(action=self._action))
        parser.add_argument(
            "--data", help="Install or upgrade data dependencies",
            dest="install_data", action="store_true", default=False)
        parser.add_argument(
            "--genomes", help="Genomes to download",
            action="append", default=[],
            choices=bcbio_config["supported.genomes"])
        parser.add_argument(
            "--aligners", help="Aligner indexes to download",
            action="append", default=[],
            choices=bcbio_config["supported.indexes"])
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
        self.defaults.add_defaults()
        # Retrieve supported remote inputs specified on the command line.
        self.defaults.retrieve()
        # Check if the datadir exists
        self.defaults.check_datadir("bcbio-nextgen not installed or "
                                    "upgrade.")
        # Add all the missing arguments related to docker image.
        self.install.image_defaults()

    def work(self):
        """Run the command with the received information."""
        container = docker_container.Docker()
        mounts = docker_common.prepare_system(self.args.datadir,
                                              constant.DOCKER["biodata_dir"])

        if self.args.wrapper:
            self._updates.append("wrapper scripts")
            if not common_utils.upgrade_bcbio_vm():
                LOG.warning("Cannot update bcbio-nextgen-vm; "
                            "not installed with conda")

        if self.args.install_tools:
            self._updates.append("bcbio-nextgen code and third party tools")
            container.pull_image(self.args.image)
            # Ensure external galaxy configuration in sync when
            # doing tool upgrade
            container.run_command(image=self.args.image, mounts=mounts,
                                  arguments=["upgrade"], ports=None)

        if self.args.install_data:
            self._updates.append("biological data")
            container.check_image(self.args.image)
            container.run_command(image=self.args.image, mounts=mounts,
                                  arguments=self._get_command_line())

        self.install.save_install_defaults()

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

    def __init__(self, parent, parser):
        super(Install, self).__init__(True, parent, parser)


class Upgrade(_Action):

    """Upgrade bcbio-nextgen docker container and data."""

    def __init__(self, parent, parser):
        super(Upgrade, self).__init__(False, parent, parser)


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
        self.defaults.add_defaults()
        # Retrieve supported remote inputs specified on the command line.
        self.defaults.retrieve()
        # Check if the datadir exists if it is required.
        self.defaults.check_datadir("Could not do upgrade of "
                                    "bcbio_system.yaml.")

    def work(self):
        """Update bcbio_system.yaml file with a given target of cores
        and memory.
        """
        container = docker_container.Docker()
        return container.update_system(datadir=self.args.datadir,
                                       cores=self.args.cores,
                                       memory=self.args.memory)


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
            default=bcbio_config["docker.image"])
        parser.set_defaults(work=self.run)

    def work(self):
        """Install python code from a bcbio-nextgen development tree
        inside of docker.
        """
        container = docker_container.Docker()
        return container.install_bcbio(image=self.args.image)

    def epilogue(self):
        """Executed once after the command running."""
        print("Updated bcbio-nextgen install in docker container: %s" %
              self.args.image)


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
        parser.add_argument(
            "-i", "--image", help="Image name to write updates to",
            default=bcbio_config["docker.image"])

        parser.set_defaults(work=self.run)

    def prologue(self):
        """Executed once before the arguments parsing."""
        # Add user configured defaults to supplied command line arguments.
        self.defaults.add_defaults()
        # Retrieve supported remote inputs specified on the command line.
        self.defaults.retrieve()
        # Check if the datadir exists if it is required.
        self.defaults.check_datadir("Could not run analysis.")
        # Add all the missing arguments related to docker image.
        self.install.image_defaults()

    def work(self):
        """Run the command with the received information."""
        container = docker_container.Docker()
        container.run_analysis(image=self.args.image,
                               sample=self.args.sample_config,
                               fcdir=self.args.fcdir,
                               config=self.args.systemconfig,
                               datadir=self.args.datadir,
                               cores=self.args.numcores)


class RunFunction(base.Command):

    """Run a specific bcbio-nextgen function with provided arguments."""

    def setup(self):
        """Extend the parser configuration in order to expose this command."""
        parser = self._parser.add_parser(
            "runfn",
            help=("Run a specific bcbio-nextgen function with provided"
                  " arguments"))
        parser.add_argument(
            "fn_name",
            help="Name of the function to run")
        parser.add_argument(
            "parallel",
            help="JSON/YAML file describing the parallel environment")
        parser.add_argument(
            "runargs",
            help="JSON/YAML file with arguments to the function")
        parser.add_argument(
            "--systemconfig",
            help=("Global YAML configuration file specifying system details. "
                  "Defaults to installed bcbio_system.yaml."))
        parser.add_argument(
            "-n", "--numcores", type=int, default=1,
            help="Total cores to use for processing")
        parser.add_argument(
            "-i", "--image", help="Image name to write updates to",
            default=bcbio_config["docker.image"])

        parser.set_defaults(work=self.run)

    def prologue(self):
        """Executed once before the arguments parsing."""
        # Add user configured defaults to supplied command line arguments.
        self.defaults.add_defaults()
        # Retrieve supported remote inputs specified on the command line.
        self.defaults.retrieve()
        # Check if the datadir exists if it is required.
        self.defaults.check_datadir("Could not run bcbio-nextgen function.")
        # Add all the missing arguments related to docker image.
        self.install.image_defaults()

    def work(self):
        """Run the command with the received information."""
        container = docker_container.Docker()
        container.run_bcbio_function(image=self.args.image,
                                     config=self.args.systemconfig,
                                     parallel=self.args.parallel,
                                     function=self.args.fn_name,
                                     args=self.args.runargs)


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
        parser.add_argument(
            "-i", "--image", help="Image name to write updates to",
            default=bcbio_config["docker.image"])

        parser.set_defaults(work=self.run)

    def prologue(self):
        """Executed once before the arguments parsing."""
        # Add user configured defaults to supplied command line arguments.
        self.defaults.add_defaults()
        # Retrieve supported remote inputs specified on the command line.
        self.defaults.retrieve()
        # Check if the datadir exists if it is required.
        self.defaults.check_datadir("Could not run server.")
        # Add all the missing arguments related to docker image.
        self.install.image_defaults()

    def work(self):
        """Run the command with the received information."""
        print("Running server on port %s. Press ctrl-c to exit." %
              self.args.port)

        container = docker_container.Docker()
        container.run_server(image=self.args.image, port=self.args.port)


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
        self.defaults.save_defaults()
