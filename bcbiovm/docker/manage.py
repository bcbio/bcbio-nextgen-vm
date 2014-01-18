"""Manage stopping and starting a docker container for running analysis.
"""
from __future__ import print_function
from __future__ import unicode_literals
import grp
import os
import pwd

def docker_cmd(image, mounts, bcbio_nextgen_cl):
    """Create command line to call docker with the supplied arguments to bcbio-nextgen.py.
    """
    mounts = " ".join("-v %s" % x for x in mounts)
    cmd = ("docker run -t {mounts} {image} "
           "/bin/bash -c '" + user_create_cmd() +
           "bcbio_nextgen.py {bcbio_nextgen_cl}"
           "\"'")
    return cmd.format(**locals())

def user_create_cmd(chown_cmd=""):
    """Create a user on the docker container with equivalent UID/GIDs to external user.
    """
    user = pwd.getpwuid(os.getuid())
    group = grp.getgrgid(os.getgid())
    container_bcbio_dir = "/usr/local/share"
    homedir = "/home/{user.pw_name}".format(**locals())
    cmd = ("addgroup --quiet --gid {group.gr_gid} {group.gr_name} && "
           "useradd -m -d {homedir} -s /bin/bash -g {group.gr_gid} -o -u {user.pw_uid} {user.pw_name} && "
           + chown_cmd +
           "su - -s /bin/bash {user.pw_name} -c \"cd {homedir} && "
           + proxy_cmd())
    return cmd.format(**locals())

def proxy_cmd():
    """Pass external proxy information inside container for retrieval.
    """
    out = "git config --global url.https://github.com/.insteadOf git://github.com/ && "
    if "HTTP_PROXY" in os.environ:
        out += "export HTTP_PROXY=%s && " % os.environ["HTTP_PROXY"]
    return out
