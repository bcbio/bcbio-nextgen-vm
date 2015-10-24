"""Utilities to help with developing using bcbio inside of docker."""
import glob
import os
import pwd

import numpy
import yaml

from bcbiovm import log as logging
from bcbiovm.common import exception
from bcbiovm.common import utils as common_utils
from bcbiovm.container.docker import mounts as docker_mounts

LOG = logging.get_logger(__name__)


def _get_system_config(config, datadir):
    """Retrieve a system configuration with galaxy references
    specified.

    :param datadir: Directory with genome data and associated
                    files.
    :param config:  Global YAML configuration file specifying system
                    details.
    """
    config_file = system_config_file(config, datadir)
    with open(config_file) as in_handle:
        config = yaml.load(in_handle)

    if "galaxy_config" not in config:
        config["galaxy_config"] = os.path.join(os.path.dirname(config_file),
                                               "universe_wsgi.ini")
    return config


def get_basedir(datadir, target_genome):
    """Retrieve base directory for uploading."""
    genome_dir = os.path.join(datadir, "genomes", "*", "*")
    for dirname in glob.glob(genome_dir):
        _, genome_dir = os.path.split(dirname)
        if genome_dir == target_genome:
            return dirname

    raise exception.NotFound(object=target_genome,
                             container=glob.glob(genome_dir))


def get_memory(memory_type, size):
    """Return the corresponding megabytes as an integer of the
    received size.
    """
    if memory_type == "memory":
        current_memory = size
    elif memory_type == "jvm_opts":
        current_memory = size[1].replace("-Xmx", "")

    return common_utils.predict_size(current_memory, convert="M")


def get_mounts(cmd_args, datadir, dockerconf):
    """Prepare a list of mountpoints required by do_runfn method."""
    mounts = ["{home}:{home}".format(home=pwd.getpwuid(os.getuid()).pw_dir)]
    mounts.extend(prepare_system(datadir, dockerconf["biodata_dir"]))
    if "sample_config" in cmd_args:
        with open(cmd_args["sample_config"]) as in_handle:
            sample_config = yaml.load(in_handle)
            _, sample_mounts = docker_mounts.update_config(sample_config,
                                                           cmd_args["fcdir"])
            mounts.extend(sample_mounts)

    if "orig_systemconfig" in cmd_args:
        orig_sconfig = system_config_file(cmd_args["orig_systemconfig"],
                                          datadir)
        mounts.append("{dirname}:{dirname}"
                      .format(dirname=os.path.dirname(orig_sconfig)))

    return mounts


def calculate_common_memory(resources):
    """Get the median memory specification, in megabytes."""
    memory = []
    for memory_type, size in resources:
        current_value = get_memory(memory_type, size)
        memory.append(current_value)

    return numpy.median(memory)


def update_memory(key, cur, target, common_mem):
    """Update memory specifications to match target.

    Handles JVM options and both megabyte and gigabyte specifications.
    `target` is in megabytes. Does not adjust down memory that is more
    than 1.5x the current common memory setting, assuming these are pre-set for
    higher memory requirements.
    """
    cur_mem, _ = get_memory(key, cur)
    if cur_mem >= common_mem * 1.5:
        return cur

    new_val = "%sM" % target
    if key == "jvm_opts":
        out = cur
        out[-1] = "-Xmx%s" % new_val
    else:
        out = new_val

    return out


def prepare_system(data_directory, biodata_directory):
    """Create set of system mountpoints to link into Docker container."""
    mounts = []
    for directory in ("genomes", "liftOver", "gemini_data", "galaxy"):
        curent_directory = os.path.normpath(os.path.realpath(
            os.path.join(data_directory, directory)))

        mounts.append("{curent}:{biodata}/{directory}".format(
            curent=curent_directory, biodata=biodata_directory,
            directory=directory))

        if not os.path.exists(curent_directory):
            os.makedirs(curent_directory)

    return mounts


def find_genome_directory(dirname):
    """Handle external non-docker installed biodata located relative to
    config directory.
    """
    mounts = []
    sam_loc = os.path.join(dirname, "tool-data", "sam_fa_indices.loc")
    genome_dirs = {}
    if os.path.exists(sam_loc):
        with open(sam_loc) as in_handle:
            for line in in_handle:
                if line.startswith("index"):
                    parts = line.split()
                    genome_dirs[parts[1].strip()] = parts[-1].strip()
    for genome_dir in sorted(list(set(genome_dirs.values()))):
        # Special case used in testing -- relative paths
        if genome_dir and not os.path.isabs(genome_dir):
            rel_genome_dir = os.path.dirname(os.path.dirname(
                os.path.dirname(genome_dir)))
            full_genome_dir = os.path.normpath(os.path.join(
                os.path.dirname(sam_loc), rel_genome_dir))
            mounts.append("%s:%s" % (full_genome_dir, full_genome_dir))
    return mounts


def system_config_file(config, datadir):
    """Retrieve system configuration file from input or default directory.

    :param datadir: Directory with genome data and associated
                    files.
    :param config:  Global YAML configuration file specifying system
                    details.
    """
    if config:
        if not os.path.isabs(config):
            return os.path.normpath(os.path.join(os.getcwd(), config))
        else:
            return config

    return os.path.join(datadir, "galaxy", "bcbio_system.yaml")


def read_system_config(config, datadir):
    """Get the system configuration and required mountpoints.

    :param datadir: Directory with genome data and associated
                    files.
    :param config:  Global YAML configuration file specifying system
                    details.
    """
    mounts = []
    config = _get_system_config(config, datadir)

    # Map external galaxy specifications over to docker container
    dirname, base = os.path.split(os.path.normpath(
        os.path.realpath(config["galaxy_config"])))
    mounts.append("{dirname}:{dirname}".format(dirname=dirname))
    mounts.extend(find_genome_directory(dirname))
    config["galaxy_config"] = os.path.join(dirname, base)

    return config, mounts


def local_system_config(config, datadir, work_dir):
    """Create a ready to run local system configuration file.

    :param datadir:  Directory with genome data and associated
                     files.
    :param config:   Global YAML configuration file specifying system
                     details.
    :param work_dir: The current working directory.
    """
    system_config = _get_system_config(config, datadir)
    system_cfile = os.path.join(work_dir, "bcbio_system-prep.yaml")
    with open(system_cfile, "w") as out_handle:
        yaml.dump(system_config, out_handle, default_flow_style=False,
                  allow_unicode=False)

    return system_cfile
