"""
Container base-class:
    (Beginning of) the contract that every container must follow,
    and shared types that support that contract.
"""
import abc

import six

from bcbiovm import config as bcbiovm_config


@six.add_metaclass(abc.ABCMeta)
class Container(object):

    """Base class for the containers."""

    @classmethod
    def _export_environment(cls):
        """Pass external proxy information inside container for retrieval."""
        output = []
        environment = bcbiovm_config["env"]
        for field in environment.fields():
            output.extend(["-e", "%s=%s" % (field, environment[field])])

        return output

    @abc.abstractmethod
    def install_bcbio(self, image):
        """Install python code from a bcbio-nextgen development tree
        inside of docker.

        :param image: The name of the image which should be used.
        """
        pass

    @abc.abstractmethod
    def build_image(self, container, cwd, full, storage, context):
        """Build an image from the current container and export it
        to the received cloud provider.

        :param container: The container name where to upload the gzipped
                          docker image to.
        :param cwd:       The working directory.
        :param full:      The type of the build. If it is True all code
                          and third party tools will be installed otherwise
                          only only bcbio-nextgen code will be copied.
        :param storage:   The storage manager required for this task.
        :param context:   A dictionary that may contain useful information
                          for the cloud provider (credentials, headers etc).
        """
        pass

    @abc.abstractmethod
    def check_image(self, image):
        """Check if the received image is available.

        :param image:  The name of the required container image.
        """
        pass

    @abc.abstractmethod
    def prepare_genomes(self, genomes, aligners, output):
        """Start with an existing set of cached data to output directory.

        :param genomes:     Genomes to download.
        :param aligners:    Aligner indexes to download.
        :param output:      The output directory.
        """
        pass

    @abc.abstractmethod
    def upload_biodata(self, genomes, aligners, image, datadir, provider,
                       context):
        """Upload pre-prepared biological data to cache.

        :param genomes:    Genomes which should be uploaded.
        :param aligners:   Aligner indexes which should be uploaded.
        :param image:      Image name to write updates to.
        :param datadir:    Directory with genome data and associated
                           files.
        :param provider:   An instance of a cloud provider
        :param context:    A dictionary that may contain useful information
                           for the cloud provider (credentials, headers etc).
        """
        pass

    @abc.abstractmethod
    def update_system(self, datadir, cores, memory):
        """Update system core and memory configuration.

        :param datadir:   Directory with genome data and associated files.
        :param cores:     Target cores to use for multi-core processes.
        :param memory:    Target memory per core, in Mb (1000 = 1Gb).
        """
        pass

    @abc.abstractmethod
    def run_analysis(self, image, sample, fcdir, config, datadir, cores):
        """Run an automated analysis on the local machine.

        :param image:   The name of the image which should be used.
        :param sample:  YAML file with details about samples to process.
        :param fcdir:   A directory of Illumina output or fastq files
                        to process.
        :param config:  Global YAML configuration file specifying system
                        details.
        :param datadir: Directory with genome data and associated
                        files.
        :param cores:   The number of cores which should be used for
                        processing.
        """
        pass

    @abc.abstractmethod
    def run_command(self, image, mounts, arguments, ports=None):
        """Run command in docker container with the supplied arguments
        to bcbio-nextgen.py.

        :param image:       The name of the image which should be used.
        :param mounts:      A list of volumes which will be bonded to
                            the container.
        :param arguments:   The arguments for the bcbio command.
        :param ports:       A list of ports that will be published from
                            container to the host.
        """
        pass

    @abc.abstractmethod
    def run_bcbio_function(self, image, config, parallel, function, args):
        """Run a specific bcbio-nextgen function with provided arguments.

        :param image:       The name of the image which should be used.
        :param config:      Global YAML configuration file specifying system
                            details.
        :param parallel:    JSON/YAML file describing the parallel environment.
        :param function:    The name of the function.
        :param args:        JSON/YAML file with arguments to the function.
        """
        pass

    @abc.abstractmethod
    def run_function(self, function, arguments, cmd_args, parallel,
                     dockerconf=None, ports=None):
        """"Run a single defined function inside a docker container,
        returning results.

        :param function:    The name of the function.
        :param arguments:   Arguments required for running the function.
        :param cmd_args:    A dictionary with aditional arguments.
        :param parallel:    Information regarding the parallel environment.
        :param dockerconf:  A dinctionary with configurations for docker.
        :param ports:       A list of ports that will be published from
                            container to the host.
        """
        pass

    @abc.abstractmethod
    def run_server(self, image, port):
        """Persistent REST server receiving requests via the specified port.

        :param image:   The name of the image which should be used.
        :param port:    External port to connect to the container image.
        """
        pass

    @abc.abstractmethod
    def pull_image(self, image):
        """Pull down latest docker image, using export uploaded to a storage
        manager.
        """
        pass
