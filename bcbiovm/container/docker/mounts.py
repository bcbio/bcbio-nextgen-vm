"""Manage external data directories mounted to a docker container.
"""
from __future__ import print_function
import os

import six

from bcbiovm import log as logging
from bcbiovm.container.docker import remap

LOG = logging.get_logger(__name__)


def update_config(config, fcdir=None):
    """Resolve relative and symlinked path, providing mappings for
    docker container.
    """
    config, directories = normalize_config(config, fcdir)
    if config.get("upload", {}).get("dir"):
        directories.append(config["upload"]["dir"])
    mounts = {}
    for _, d in enumerate(sorted(set(directories))):
        mounts[d] = d
    mounts = ["%s:%s" % (k, v) for k, v in mounts.items()]
    config = remap.external_to_docker(config, mounts)
    return config, mounts


def normalize_config(config, fcdir=None):
    """Normalize sample configuration file to have absolute paths and collect
    directories.

    Prepares configuration for remapping directories into docker containers.
    """
    LOG.debug("Normalize sample configuration: %s", config)

    absdetails = []
    directories = []
    ignore = ["variantcaller", "realign", "recalibrate", "phasing", "svcaller"]

    for details in config["details"]:
        details = abs_file_paths(details, base_dirs=[fcdir] if fcdir else None,
                                 ignore=["description", "analysis", "lane",
                                         "resources", "genome_build"])
        details["algorithm"] = abs_file_paths(
            details["algorithm"], base_dirs=[fcdir] if fcdir else None,
            ignore=ignore)
        absdetails.append(details)
        directories.extend(_get_directories(details, ignore))

    if config.get("upload", {}).get("dir", None):
        config["upload"]["dir"] = os.path.normpath(os.path.realpath(
            os.path.join(os.getcwd(), config["upload"]["dir"])))
        if not os.path.exists(config["upload"]["dir"]):
            os.makedirs(config["upload"]["dir"])

    config["details"] = absdetails
    return config, directories


def _get_directories(xs, ignore):
    """Retrieve all directories specified in an input file."""
    out = []
    if not isinstance(xs, dict):
        return out
    for k, v in xs.items():
        if k not in ignore:
            if isinstance(v, dict):
                out.extend(_get_directories(v, ignore))
            elif v and isinstance(v, six.string_types):
                if os.path.exists(v) and os.path.isabs(v):
                    out.append(os.path.dirname(v))
            elif v and isinstance(v, (list, tuple)):
                if os.path.exists(v[0]) and os.path.isabs(v[0]):
                    out.extend(os.path.dirname(x) for x in v)
    out = [x for x in out if x]
    return out


def _normalize_path(x, base_dirs):
    for base_dir in base_dirs:
        if os.path.exists(os.path.join(base_dir, x)):
            return os.path.normpath(os.path.realpath(
                os.path.join(base_dir, x)))
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
        if (k not in ignore_keys and v and isinstance(v, six.string_types) and
                _normalize_path(v, base_dirs)):
            out[k] = _normalize_path(v, base_dirs)

        elif (k not in ignore_keys and v and isinstance(v, (list, tuple)) and
              _normalize_path(v[0], base_dirs)):
            out[k] = [_normalize_path(x, base_dirs) for x in v]
        else:
            out[k] = v
    return out
