"""Remap external files and directories to provide docker access.

Handles identification of files from a YAML style file that need access, remapping into
docker and remapping results from docker.
"""
from __future__ import print_function

import six

def external_to_docker(xs, mount_strs):
    """Remap external files to point to internal docker container mounts.
    """
    return _remap_all_mounts(xs, _mounts_to_in_dict(mount_strs))

def docker_to_external(xs, mount_strs):
    """Remap internal docker files to point to external mounts.
    """
    return _remap_all_mounts(xs, _mounts_to_out_dict(mount_strs))

def _mounts_to_in_dict(mounts):
    """Convert docker-style mounts (external_dir):{docker_dir} into dictionary of external to docker.
    """
    out = {}
    for m in mounts:
        external, docker = m.split(":")
        out[external] = docker
    return out

def _mounts_to_out_dict(mounts):
    """Convert docker-style mounts (external_dir):{docker_dir} into dictionary of docker to external.
    """
    out = {}
    for m in mounts:
        external, docker = m.split(":")
        out[docker] = external
    return out

def _remap_mount(fname, mounts):
    """Remap a filename given potential remapping mount points.
    """
    matches = []
    for k, v in mounts.items():
        if fname.startswith(k):
            matches.append((k, v))
    matches.sort(key=lambda x: len(x[0]), reverse=True)
    remap_orig, remap_new = matches[0]
    return fname.replace(remap_orig, remap_new)

def _remap_all_mounts(xs, mounts):
    """Recursively remap any files in the input present in the mount dictionary.

    xs is a JSON-like structure with lists, and dictionaries. This recursively
    calculates files nested inside these structures.
    """
    if isinstance(xs, (list, tuple)):
        return [_remap_all_mounts(x, mounts) for x in xs]
    elif isinstance(xs, dict):
        out = {}
        for k, v in xs.items():
            out[k] = _remap_all_mounts(v, mounts)
        return out
    elif xs and isinstance(xs, six.string_types) and xs.startswith(tuple(mounts.keys())):
        return _remap_mount(xs, mounts)
    else:
        return xs
