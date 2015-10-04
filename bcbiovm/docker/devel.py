"""Utilities to help with developing using bcbio inside of docker."""
from __future__ import print_function

import copy
import glob
import os

import numpy
import yaml

from bcbio import utils
from bcbio.distributed import objectstore
from bcbio.pipeline import genome
from bcbio.provenance import do

from bcbiovm.common import cluster as clusterops
from bcbiovm.common import constant
from bcbiovm.common import exception
from bcbiovm.common import utils as common_utils
from bcbiovm.docker import manage as docker_manage
from bcbiovm.provider import factory as provider_factory

LOG = common_utils.get_logger(__name__)


def _get_memory(memory_type, size):
    """Return the corresponding megabytes as an integer of the
    received size.
    """
    if memory_type == "memory":
        current_memory = size
    elif memory_type == "jvm_opts":
        current_memory = size[1].replace("-Xmx", "")

    return common_utils.predict_size(current_memory, convert="M")


def _calculate_common_memory(resources):
    """Get the median memory specification, in megabytes."""
    memory = []
    for memory_type, size in resources:
        current_value = _get_memory(memory_type, size)
        memory.append(current_value)

    return numpy.median(memory)


def _update_memory(key, cur, target, common_mem):
    """Update memory specifications to match target.

    Handles JVM options and both megabyte and gigabyte specifications.
    `target` is in megabytes. Does not adjust down memory that is more
    than 1.5x the current common memory setting, assuming these are pre-set for
    higher memory requirements.
    """
    cur_mem, orig_mod = _get_memory(key, cur)
    if cur_mem >= common_mem * 1.5:
        return cur

    new_val = "%sM" % target
    if key == "jvm_opts":
        out = cur
        out[-1] = "-Xmx%s" % new_val
    else:
        out = new_val

    return out


def _get_basedir(datadir, target_genome):
    """Retrieve base directory for uploading."""
    genome_dir = os.path.join(datadir, "genomes", "*", "*")
    for dirname in glob.glob(genome_dir):
        basedir, genome_dir = os.path.split(dirname)
        if genome_dir == target_genome:
            return dirname

    raise exception.NotFound(object=target_genome,
                             container=glob.glob(genome_dir))


def run_docker_build(container, build_type, run_directory, provider,
                     account_name):
    """Build a new docker image.

    :param container:      The container name where to upload the gzipped
                           docker image to.
    :param build_type:     Type of docker build to do. full is all code and
                           third party tools. code is only bcbio-nextgen code.
    :param run_directory:  Directory to run docker build in.
    :param provider:       The name of the cloud provider.
    :param account_name:   The storage account name. All access to Azure
                           Storage is done through a storage account.
    """

    def extra_vars(_):
        """Extra variables to inject into a playbook."""
        return {
            "bcbio_container": container,
            "docker_buildtype": build_type,
            "bcbio_dir": run_directory,
            "provider": provider,
            "account_name": account_name,
        }

    playbook = clusterops.AnsiblePlaybook(
        extra_vars=extra_vars,
        playbook_path=provider_factory.get_playbook("docker_local"),
        inventory_path=os.path.join(constant.PATH.ANSIBLE_BASE,
                                    "standard_hosts.txt")
    )

    return playbook.run()


def run_setup_install(image):
    """Install python code from a bcbio-nextgen development tree
    inside of docker.

    :param image: Image name to write updates to.
    """
    bash_command = (
        # Remove the bcbio package from conda environment
        "rm -rf /usr/local/share/bcbio-nextgen/anaconda/lib/python2.7/"
        "site-packages/bcbio",
        # Change directory to /tmp/bcbio-nextgen
        "cd /tmp/bcbio-nextgen",
        # Run the setup.py script
        "/usr/local/share/bcbio-nextgen/anaconda/bin/python setup.py install",
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
    do.run(["docker", "attach", "--no-stdin", container], log_stdout=True,
           descr="Running in docker container: %s" % container)

    # Create a new image from a container's changes
    common_utils.execute(["docker", "commit", container, image],
                         check_exit_code=True)

    # Remove the old docker container
    common_utils.execute(["docker", "rm", container], check_exit_code=True)


def run_system_update(datadir, cores, memory):
    """Update bcbio_system.yaml file with a given target of cores
    and memory.

    :param datadir: Directory with genome data and associated files.
    :param cores:   Target cores to use for multi-core processes.
    :param memory:  Target memory per core, in Mb (1000 = 1Gb)
    """
    # Update bcbio_system.yaml
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

    median_memory = _calculate_common_memory(memory_list)
    for prog, attributes in config.get("resources", {}).iteritems():
        for key, value in attributes.iteritems():
            if key == "cores":
                output['resources'][prog][key] = int(cores)

            elif key in memory_types:
                output["resources"][prog][key] = _update_memory(
                    key, value, memory, median_memory)

    common_utils.backup(system_file, delete=True)
    with open(system_file, "w") as output_handle:
        yaml.safe_dump(output, output_handle, default_flow_style=False,
                       allow_unicode=False)


def prepare_genomes(genomes, aligners, prepped):
    """Start with an existing set of cached data to output directory.

    :param genomes:     Genomes to download.
    :param aligners:    Aligner indexes to download.
    """
    aligners.append("samtools")
    for genome_build in genomes:
        LOG.info("Preparing %s", genome_build)
        for target in aligners:
            genome.download_prepped_genome(genome_build=genome_build, data={},
                                           name=target, need_remap=False,
                                           out_dir=prepped)
            LOG.info("Downloaded prepped %s to %s. Edit and re-run without "
                     "--prepped to upload" % (genome_build, prepped))


def upload_biodata(genome_build, target, all_dirs):
    """Upload biodata for a specific genome build and target to a storage
    manager.
    """
    storage_manager = objectstore.AmazonS3()
    biodata_info = objectstore.BIODATA_INFO["s3"].format(build=genome_build,
                                                         target=target)

    want_dirs = ("rnaseq", "seq", "variation", "vep", "snpeff")
    command = ("bcbiovm.py tool upload {provider} {arguments}")
    message = ("Upload pre-prepared genome data: %(genome)s, %(target)s: "
               "{directory}" % {"genome": genome_build, "target": "target"})

    if target == "seq":
        target_dirs = [directory for directory in all_dirs
                       if directory.startswith("rnaseq-")
                       or directory in want_dirs]
    else:
        target_dirs = [target] if target in all_dirs else []

    file_info = storage_manager.parse_remote(biodata_info)
    if not storage_manager.exists(file_info.bucket, file_info.key):
        return

    archive = common_utils.compress(target_dirs)
    arguments = ["--file", archive, "--key", file_info.key,
                 "--bucket", file_info.bucket]
    do.run(command.format(provider="aws", arguments=" ".join(arguments)),
           message.format(directory=directory))
    os.remove(archive)


def run_biodata_upload(genomes, aligners, image, mounts, datadir):
    """Manage preparation of biodata on a local machine, uploading
    to a storage manager in pieces.

    :param genomes:     Genomes to download.
    :param aligners:    Aligner indexes to download.
    :param image:       Image name to write updates to.
    :param datadir:     Directory with genome data and associated
                        files.
    """
    for genome_build in genomes:
        command_line = ["upgrade", "--genomes", genome_build]
        for aligner in aligners:
            command_line.extend(["--aligners", aligner])

        docker_manage.run_bcbio_cmd(image, mounts, command_line)
        basedir, _ = _get_basedir(datadir, genome_build)
        LOG.debug("Uploading %s" % genome_build)

        with utils.chdir(basedir):
            all_dirs = sorted(os.listdir(genome_build))
            upload_biodata(genome_build=genome_build, target="seq",
                           all_dirs=all_dirs)

            for aligner in aligners:
                upload_biodata(
                    genome_build=genome_build, all_dirs=all_dirs,
                    target=genome.REMAP_NAMES.get(aligner, aligner))
