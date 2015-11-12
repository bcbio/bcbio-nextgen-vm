"""Support running bcbio-nextgen inside of isolated docker containers."""
import copy
import grp
import os
import platform
import pwd
import subprocess
import uuid

import yaml
from bcbio import utils as bcbio_utils
from bcbio.pipeline import genome as bcbio_genome
from bcbio.provenance import do as bcbio_do
from bcbio import log as bcbio_log

from bcbiovm import config as bcbio_config
from bcbiovm import log as logging
from bcbiovm.common import constant
from bcbiovm.common import cluster as clusterops
from bcbiovm.common import exception
from bcbiovm.common import utils as common_utils
from bcbiovm.container import base
from bcbiovm.container.docker import common as docker_common
from bcbiovm.container.docker import mounts as docker_mounts
from bcbiovm.container.docker import remap as docker_remap
from bcbiovm.provider import factory as provider_factory
from bcbiovm.provider.common import playbook as common_playbook

LOG = logging.get_logger(__name__)


class Docker(base.Container):

    """Support running bcbio-nextgen inside of isolated docker containers."""

    def __init__(self):
        super(Docker, self).__init__()
        self._config = {
            "port": 8085,
            "biodata_dir": "/usr/local/share/bcbio-nextgen",
            "work_dir": "/mnt/work",
            "image_url": "bcbio/bcbio",
        }
        self._playbook = common_playbook.Playbook()

    @classmethod
    def _kill_container(cls, container_id):
        """Kill a running container."""
        # Get the runnning containers list
        output, _ = common_utils.execute(["docker", "ps"])
        running = [line.split()[0] for line in output.splitlines()]
        if container_id in running:
            _, error = common_utils.execute(("docker", "kill", container_id),
                                            check_exit_code=False)
            if error:
                LOG.error(error)
                return False

        return True

    @classmethod
    def install_bcbio(cls, image):
        """Install python code from a bcbio-nextgen development tree
        inside of docker.

        :param image: The name of the image which should be used.
        """
        bash_command = (
            # Remove the bcbio package from conda environment
            "rm -rf /usr/local/share/bcbio-nextgen/anaconda/lib/python2.7/"
            "site-packages/bcbio",
            # Change directory to /tmp/bcbio-nextgen
            "cd /tmp/bcbio-nextgen",
            # Run the setup.py script
            "/usr/local/share/bcbio-nextgen/anaconda/bin/python "
            "setup.py install",
        )

        command = [
            "docker", "run", "-i", "-d", "--net=host",
            "-v", "%s:%s" % (os.getcwd(), "/tmp/bcbio-nextgen"),
            image, "bash", "-l", "-c", " && ".join(bash_command)
        ]

        # Remove the old version of the code base from the docker image
        # and install the bcbio-nextgen from the development tree
        output, _ = common_utils.execute(command)
        container = output.strip()

        # Attach to a running container
        bcbio_do.run(["docker", "attach", "--no-stdin", container],
                     log_stdout=True,
                     descr="Running in docker container: %s" % container)

        # Create a new image from a container's changes
        common_utils.execute(["docker", "commit", container, image],
                             check_exit_code=True)

        # Remove the old docker container
        common_utils.execute(["docker", "rm", container],
                             check_exit_code=True)

    def build_image(self, cwd, full):
        """Build an image from the current container and export it
        to the received cloud provider.

        :param cwd:       The working directory.
        :param full:      The type of the build. If it is True all code
                          and third party tools will be installed otherwise
                          only only bcbio-nextgen code will be copied.
        """

        def extra_vars(_):
            """Extra variables to inject into a playbook."""
            return {
                "docker_buildtype": "full" if full else "code",
                "docker_image": bcbio_config["docker.bcbio_image"],
                "bcbio_dir": cwd,
                "bcbio_repo": bcbio_config["bcbio.repo"],
                "bcbio_branch": bcbio_config["bcbio.branch"],
            }

        docker_image = os.path.join(cwd, bcbio_config["docker.bcbio_image"])
        LOG.debug("Creating docker image: %s", docker_image)

        playbook = clusterops.AnsiblePlaybook(
            extra_vars=extra_vars,
            playbook_path=self._playbook.docker_local,
            inventory_path=os.path.join(constant.PATH.ANSIBLE_BASE,
                                        "standard_hosts.txt")
        )
        playbook_response = playbook.run()
        LOG.debug("Playbook response: %s", playbook_response)

        if not os.path.exists(docker_image):
            LOG.warning("Failed to create docker image.")
            return False

        return docker_image

    @classmethod
    def check_image(cls, image):
        """Check if the received image is available.

        :param image:  The name of the required container image.
        """
        output, _ = common_utils.execute(["docker", "images"],
                                         check_exit_code=0)
        for line in output.splitlines():
            parts = line.split()
            if len(parts) > 1 and parts[0] == image:
                return

        raise exception.NotFound(object="docker image %s" % image,
                                 container="local repository")

    @classmethod
    def upload_image(cls, path, container, storage, context):
        """Upload the image to the received file storage.

        :param path:      The path of the docker image file.
        :param container: The container name where to upload the gzipped
                          docker image to.
        :param storage:   The storage manager required for this task.
        :param context:   A dictionary that may contain useful information
                          for the storage manager (credentials, headers etc).
        """
        return storage.upload(path=path, container=container,
                              filename=bcbio_config["docker.bcbio_image"],
                              context=context)

    @classmethod
    def prepare_genomes(cls, genomes, aligners, output):
        """Start with an existing set of cached data to output directory.

        :param genomes:     Genomes to download.
        :param aligners:    Aligner indexes to download.
        :param output:      The output directory.
        """
        aligners.append("samtools")
        for genome_build in genomes:
            LOG.info("Preparing %(genome)s", {"genome": genome_build})
            for target in aligners:
                bcbio_genome.download_prepped_genome(
                    genome_build=genome_build, data={}, out_dir=output,
                    name=target, need_remap=False)

                LOG.info("Downloaded prepped %s to %s. Edit and re-run without"
                         " --prepped to upload", genome_build, output)

    @classmethod
    def upload_biodata(cls, genomes, aligners, image, datadir, provider,
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
        wanted_dirs = ("rnaseq", "seq", "variation", "vep", "snpeff")
        mounts = docker_common.prepare_system(datadir,
                                              constant.DOCKER["biodata_dir"])

        for genome_build in genomes:
            command_line = ["upgrade", "--genomes", genome_build]
            for aligner in aligners:
                command_line.extend(["--aligners", aligner])

            cls.run_command(image, mounts, command_line)
            LOG.debug("Uploading %(genome)s", {"genome": genome_build})

            basedir = docker_common.get_basedir(datadir, genome_build)
            with bcbio_utils.chdir(basedir):
                all_dirs = sorted(os.listdir(genome_build))
                provider.upload_biodata(
                    genome_build=genome_build, target="seq",
                    source=[dirname for dirname in all_dirs
                            if dirname.startswith("rnaseq-") or
                            dirname in wanted_dirs],
                    context=context)

                for aligner in aligners:
                    target = bcbio_genome.REMAP_NAMES.get(aligner, aligner)
                    provider.upload_biodata(
                        genome_build=genome_build, target=target,
                        source=[target] if target in all_dirs else [],
                        context=context)

    def update_system(self, datadir, cores, memory):
        """Update system core and memory configuration.

        :param datadir:   Directory with genome data and associated files.
        :param cores:     Target cores to use for multi-core processes.
        :param memory:    Target memory per core, in Mb (1000 = 1Gb).
        """
        memory_types = ("memory", "jvm_opts")
        memory_list = []

        system_file = os.path.join(datadir, "galaxy", "bcbio_system.yaml")
        with open(system_file) as in_handle:
            config = yaml.safe_load(in_handle)

        output = copy.deepcopy(config)
        for attributes in config.get("resources", {}).itervalues():
            for key, value in attributes.iteritems():
                if key in memory_types:
                    memory_list.append((key, value))

        median_memory = docker_common.calculate_common_memory(memory_list)
        for prog, attributes in config.get("resources", {}).iteritems():
            for key, value in attributes.iteritems():
                if key == "cores":
                    output['resources'][prog][key] = int(cores)

                elif key in memory_types:
                    value = docker_common.update_memory(key, value, memory,
                                                        median_memory)
                    output["resources"][prog][key] = value

        common_utils.backup(system_file, delete=True)
        with open(system_file, "w") as output_handle:
            yaml.safe_dump(output, output_handle, default_flow_style=False,
                           allow_unicode=False)

    @classmethod
    def run_command(cls, image, mounts, arguments, ports=None):
        """Run command in docker container with the supplied arguments
        to bcbio-nextgen.py.

        :param image:       The name of the image which should be used.
        :param mounts:      A list of volumes which will be bonded to
                            the container.
        :param arguments:   The arguments for the bcbio command.
        :param ports:       A list of ports that will be published from
                            container to the host.

        Notes:
            On Mac OSX boot2docker runs the docker server inside VirtualBox,
            which maps the root user there to the external user.

            In this case we want to run the job as root so it will have
            permission to access user directories. Since the Docker server
            is sandboxed inside VirtualBox this doesn't have the same security
            worries as on a Linux system.

            On Linux systems, we run commands as the original calling user so
            they have the same permissions inside the Docker container as they
            do externally.
        """
        user = pwd.getpwuid(os.getuid())
        group = grp.getgrgid(os.getgid())

        command = ["docker", "run", "-d", "-i"]
        if bcbio_config.get("env.BCBIO_DOCKER_PRIVILEGED", False):
            command.append("--privileged")
        # Use host-networking so Docker works correctly on AWS VPCs
        command.append("--net=host")
        for port in ports or ():
            command.extend(("-p", port))
        for mount_point in mounts:
            command.extend(("-v", mount_point))

        command.extend(cls._export_environment())
        command.extend(("-e", "PERL5LIB=/usr/local/lib/perl5"))
        command.append(image)

        if platform.system() != "Darwin":
            command.extend(["/sbin/createsetuser", user.pw_name,
                            str(user.pw_uid), group.gr_name,
                            str(group.gr_gid)])
        command.append("bcbio_nextgen.py")
        command.extend(arguments)

        cid, _ = common_utils.execute(command)
        cid = cid.strip()

        try:
            bcbio_do.run(["docker", "attach", "--no-stdin", cid],
                         "Running in docker container: %s" % cid,
                         log_stdout=True)
        except subprocess.CalledProcessError as exc:
            raise exception.BCBioException(exc)
        finally:
            LOG.warning("Stopping docker container")
            cls._kill_container(cid)
            _, error = common_utils.execute(("docker", "rm", cid),
                                            check_exit_code=False)
            if error:
                LOG.error(error)

        return cid

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
        bcbio_log.setup_local_logging({"include_time": False})
        work_dir = os.getcwd()
        bcbio_sample = os.path.join(work_dir, "bcbio_sample-forvm.yaml")
        bcbio_system = os.path.join(work_dir, "bcbio_system-forvm.yaml")

        # Get system and sample configurations and mountpoints.
        with open(sample) as in_handle:
            sample_config, sample_mounts = docker_mounts.update_config(
                yaml.load(in_handle), fcdir)
        system_config, system_mounts = docker_common.read_system_config(
            config, datadir)

        # Dump the configurations on the new locations
        with open(bcbio_sample, "w") as out_handle:
            yaml.dump(sample_config, out_handle, default_flow_style=False,
                      allow_unicode=False)
        with open(bcbio_system, "w") as out_handle:
            yaml.dump(system_config, out_handle, default_flow_style=False,
                      allow_unicode=False)

        # Prepare the mountpoints list
        mounts = docker_common.prepare_system(datadir,
                                              self._config["biodata_dir"])
        mounts.append("%s:%s" % (work_dir, self._config["work_dir"]))
        mounts.extend(sample_mounts)
        mounts.extend(system_mounts)

        # Prepare the arguments for bcbio command
        arguments = (
            os.path.join(self._config["workdir"], "bcbio_sample-forvm.yaml"),
            os.path.join(self._config["workdir"], "bcbio_system-forvm.yaml"),
            "--numcores", str(cores),
            "--workdir={workdir}".format(workdir=self._config["work_dir"])
        )

        # Run the command
        self.run_command(image=image, mounts=mounts, arguments=arguments)

    def run_bcbio_function(self, image, config, parallel, function, args):
        """Run a specific bcbio-nextgen function with provided arguments.

        :param image:       The name of the image which should be used.
        :param config:      Global YAML configuration file specifying system
                            details.
        :param parallel:    JSON/YAML file describing the parallel environment.
        :param function:    The name of the function.
        :param args:        JSON/YAML file with arguments to the function.
        """
        with open(parallel) as in_handle:
            parallel = yaml.safe_load(in_handle)

        with open(args) as in_handle:
            runargs = yaml.safe_load(in_handle)

        provider = parallel["pack"]["type"]
        ship = provider_factory.get_ship(provider)
        shipping_config = provider_factory.get_ship_config(provider, raw=False)
        cmd_args = {
            "systemconfig": config,
            "image": image,
            "pack": shipping_config(parallel["pack"])
        }

        result = self.run_function(function=function, arguments=runargs,
                                   cmd_args=cmd_args, parallel=parallel,
                                   dockerconf=self._config)

        out_file = "%s-out%s" % os.path.splitext(args)
        with open(out_file, "w") as out_handle:
            yaml.safe_dump(result, out_handle, default_flow_style=False,
                           allow_unicode=False)
        ship.pack.send_output(shipping_config(parallel["pack"]), out_file)

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
        dockerconf = dockerconf or self._config

        ship = provider_factory.get_ship(cmd_args["pack"].type)
        reconstitute = ship.reconstitute()
        datadir, arguments = reconstitute.prepare_datadir(cmd_args["pack"],
                                                          arguments)
        work_dir, arguments, finalizer = reconstitute.prepare_workdir(
            cmd_args["pack"], parallel, arguments)

        reconstitute.prep_systemconfig(datadir, arguments)
        _, system_mounts = docker_common.read_system_config(
            cmd_args["systemconfig"], datadir)

        mounts = docker_common.get_mounts(cmd_args, datadir, dockerconf)
        mounts.append("%s:%s" % (work_dir, dockerconf["work_dir"]))
        mounts.extend(system_mounts)

        argfile = os.path.join(work_dir, "runfn-%s-%s.yaml" %
                               (function, uuid.uuid4()))
        with open(argfile, "w") as out_handle:
            yaml.safe_dump(docker_remap.external_to_docker(arguments, mounts),
                           out_handle, default_flow_style=False,
                           allow_unicode=False)

        outfile = "%s-out%s" % os.path.splitext(argfile)
        docker_argfile = os.path.join(dockerconf["work_dir"],
                                      os.path.basename(argfile))
        self.run_command(image=cmd_args["image"], mounts=mounts,
                         arguments=["runfn", function, docker_argfile],
                         ports=ports)

        if not os.path.exists(outfile):
            raise exception.BCBioException("Subprocess in docker container"
                                           " failed.")

        with open(outfile) as in_handle:
            out = docker_remap.docker_to_external(yaml.safe_load(in_handle),
                                                  mounts)
        out = finalizer(out)
        for each_file in (argfile, outfile):
            if os.path.exists(each_file):
                os.remove(each_file)

        return out

    def run_server(self, image, port):
        """Persistent REST server receiving requests via the specified port.

        :param image:   The name of the image which should be used.
        :param port:    External port to connect to the container image.
        """
        ports = ["%s:%s" % (port, self._config["port"])]
        self.run_command(
            image=image, mounts=[], ports=ports,
            arguments=["server", "--port", str(self._config["port"])])

    def pull_image(self, image):
        """Pull down latest docker image, using export uploaded to a storage
        manager.

        Long term plan is to use the docker index server but upload size is
        currently smaller with an exported gzipped image.
        """
        LOG.info("Retrieving bcbio-nextgen docker image with code and tools")
        if not image:
            raise exception.BCBioException("Unspecified image name for "
                                           "docker import")

        common_utils.execute(["docker", "pull", image], check_exit_code=0)
