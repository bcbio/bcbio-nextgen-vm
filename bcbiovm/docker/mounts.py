"""Manage external data directories mounted to a docker container.
"""
from __future__ import print_function
import os

import six

from bcbiovm.docker import remap

def prepare_system(datadir, docker_biodata_dir):
    """Create set of system mountpoints to link into Docker container.
    """
    mounts = []
    for d in ["genomes", "liftOver", "gemini_data", "galaxy"]:
        cur_d = os.path.normpath(os.path.realpath(os.path.join(datadir, d)))
        if not os.path.exists(cur_d):
            os.makedirs(cur_d)
        mounts.append("{cur_d}:{docker_biodata_dir}/{d}".format(**locals()))
    return mounts

def update_config(config, input_dir, fcdir=None):
    """Update input configuration with local docker container mounts.
    Maps input files into docker mounts and resolved relative and symlinked paths.
    """
    config, directories = normalize_config(config, fcdir)
    if config.get("upload", {}).get("dir"):
        config["upload"]["dir"] = os.path.normpath(os.path.realpath(
            os.path.join(os.getcwd(), config["upload"]["dir"])))
        if not os.path.exists(config["upload"]["dir"]):
            os.makedirs(config["upload"]["dir"])
        directories.append(config["upload"]["dir"])
    mounts = {}
    for i, d in enumerate(sorted(set(directories))):
        mounts[d] = os.path.join(input_dir, str(i))
    mounts = ["%s:%s" % (k, v) for k, v in mounts.items()]
    config = remap.external_to_docker(config, mounts)
    return config, mounts

def normalize_config(config, fcdir=None):
    """Normalize sample configuration file to have absolute paths and collect directories.

    Prepares configuration for remapping directories into docker containers.
    """
    absdetails = []
    directories = []
    for d in config["details"]:
        d = abs_file_paths(d, base_dirs=[fcdir] if fcdir else None,
                           ignore=["description", "analysis", "resources",
                                   "genome_build", "lane"])
        d["algorithm"] = abs_file_paths(d["algorithm"], base_dirs=[fcdir] if fcdir else None,
                                        ignore=["variantcaller", "realign", "recalibrate",
                                                "phasing", "svcaller"])
        absdetails.append(d)
        directories.extend(_get_directories(d))
    config["details"] = absdetails
    return config, directories

def find_genome_directory(dirname, container_dir):
    """Handle external non-docker installed biodata located relative to config directory.

    Need a general way to handle mounting these and adjusting paths, but this handles
    the special case used in testing.
    """
    mounts = []
    sam_loc = os.path.join(dirname, "tool-data", "sam_fa_indices.loc")
    genome_dir = None
    if os.path.exists(sam_loc):
        with open(sam_loc) as in_handle:
            for line in in_handle:
                if line.startswith("index"):
                    genome_dir = line.split()[-1].strip()
                    break
    if genome_dir and not os.path.isabs(genome_dir):
        rel_genome_dir = os.path.dirname(os.path.dirname(os.path.dirname(genome_dir)))
        mounts.append("%s:%s" % (os.path.normpath(os.path.join(os.path.dirname(sam_loc), rel_genome_dir)),
                                 os.path.normpath(os.path.join(os.path.join(container_dir, "tool-data"),
                                                               rel_genome_dir))))
    return mounts

def _get_directories(xs):
    """Retrieve all directories specified in an input file.
    """
    out = []
    if not isinstance(xs, dict):
        return out
    for k, v in xs.items():
        if isinstance(v, dict):
            out.extend(_get_directories(v))
        elif v and isinstance(v, six.string_types) and os.path.exists(v) and os.path.isabs(v):
            out.append(os.path.dirname(v))
        elif v and isinstance(v, (list, tuple)) and os.path.exists(v[0]):
            out.extend(os.path.dirname(x) for x in v)
    return out

def _normalize_path(x, base_dirs):
    for base_dir in base_dirs:
        if os.path.exists(os.path.join(base_dir, x)):
            return os.path.normpath(os.path.realpath(os.path.join(base_dir, x)))
    return None

def abs_file_paths(xs, base_dirs=None, ignore=None):
    """Expand files to be absolute, non-symlinked file paths.
    """
    if not isinstance(xs, dict):
        return xs
    base_dirs = base_dirs if base_dirs else []
    base_dirs.append(os.getcwd())
    ignore_keys = set(ignore if ignore else [])
    out = {}
    for k, v in xs.items():
        if k not in ignore_keys and v and isinstance(v, six.string_types) and _normalize_path(v, base_dirs):
            out[k] = _normalize_path(v, base_dirs)
        elif k not in ignore_keys and v and isinstance(v, (list, tuple)) and _normalize_path(v[0], base_dirs):
            out[k] = [_normalize_path(x, base_dirs) for x in v]
        else:
            out[k] = v
    return out
