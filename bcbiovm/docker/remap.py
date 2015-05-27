"""Remap external files and directories to provide docker access.

Handles identification of files from a YAML style file that need access,
remapping into docker and remapping results from docker.
"""
from __future__ import print_function
import os

from bcbio.pipeline import run_info

import six


def external_to_docker(xs, mount_strs):
    """Remap external files to point to internal docker container mounts."""
    return walk_files(xs, remap_fname, _mounts_to_in_dict(mount_strs))


def docker_to_external(xs, mount_strs):
    """Remap internal docker files to point to external mounts.
    """
    return walk_files(xs, remap_fname, _mounts_to_out_dict(mount_strs))


def _mounts_to_in_dict(mounts):
    """Convert docker-style mounts (external_dir):{docker_dir} into dictionary
    of external to docker.
    """
    out = {}
    for m in mounts:
        external, docker = m.split(":")
        out[external] = docker
    return out


def _mounts_to_out_dict(mounts):
    """Convert docker-style mounts (external_dir):{docker_dir} into dictionary
    of docker to external.
    """
    out = {}
    for m in mounts:
        external, docker = m.split(":")
        out[docker] = external
    return out


def remap_fname(fname, context, remap_dict):
    """Remap a filename given potential remapping mount points."""
    # FIXME(alexandrucoman): Unused argument 'context'
    # pylint: disable=unused-argument

    matches = []
    for k, v in remap_dict.items():
        if fname.startswith(k):
            matches.append((k, v))
    matches.sort(key=lambda x: len(x[0]), reverse=True)
    remap_orig, remap_new = matches[0]
    return fname.replace(remap_orig, remap_new)


def walk_files(xs, f, remap_dict, context=None, pass_dirs=False):
    """Walk a set of input arguments, calling f on any files in the given
    remapping dictionary.

    xs is a JSON-like structure with lists, and dictionaries. This recursively
    calculates files nested inside these structures.

    context keeps track of the nested set of keys associated with a file.
    """
    if isinstance(xs, (list, tuple)):
        return [walk_files(x, f, remap_dict, context, pass_dirs) for x in xs]
    elif isinstance(xs, dict):
        out = {}
        if context is None:
            context = []
        for k, v in xs.items():
            if (context and context[-1] == "algorithm"
                    and k in run_info.ALGORITHM_NOPATH_KEYS):
                out[k] = v
            else:
                cur_context = context[:] + [k]
                out[k] = walk_files(v, f, remap_dict, cur_context, pass_dirs)
        return out
    elif (xs and isinstance(xs, six.string_types)
          and xs.startswith(tuple(remap_dict.keys()))):
        return f(xs, context, remap_dict)
    elif (xs and isinstance(xs, six.string_types) and os.path.exists(xs) and
          (os.path.isfile(xs) or pass_dirs) and not remap_dict):
        return f(xs, context, remap_dict)
    else:
        return xs
