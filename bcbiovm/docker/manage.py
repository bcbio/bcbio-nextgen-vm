"""Manage stopping and starting a docker container for running analysis.
"""
from __future__ import print_function
import grp
import os
import pwd
import subprocess

def run_bcbio_cmd(image, mounts, bcbio_nextgen_cl):
    """Run command in docker container with the supplied arguments to bcbio-nextgen.py.
    """
    mounts = " ".join("-v %s" % x for x in mounts)
    cmd = ("docker run -d -i -t {mounts} {image} "
           "/bin/bash -c '" + user_create_cmd() +
           "bcbio_nextgen.py {bcbio_nextgen_cl}"
           "\"'")
    process = subprocess.Popen(cmd.format(**locals()), shell=True, stdout=subprocess.PIPE)
    cid = process.communicate()[0].strip()
    try:
        print("Running in docker container: %s" % cid)
        subprocess.call("docker attach -nostdin %s" % cid, shell=True)
    except:
        print ("Stopping docker container")
        subprocess.call("docker kill %s" % cid, shell=True)

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
    for proxyenv in ["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy",
                     "ALL_PROXY", "all_proxy"]:
        if proxyenv in os.environ:
            out += "export %s=%s && " % (proxyenv, os.environ[proxyenv])
    return out
