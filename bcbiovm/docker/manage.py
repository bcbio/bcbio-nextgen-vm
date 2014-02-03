"""Manage stopping and starting a docker container for running analysis.
"""
from __future__ import print_function
import grp
import os
import pwd
import socket
import subprocess

from bcbio.provenance import do

def run_bcbio_cmd(image, mounts, bcbio_nextgen_cl, ports=None):
    """Run command in docker container with the supplied arguments to bcbio-nextgen.py.
    """
    mounts = " ".join("-v %s" % x for x in mounts)
    ports = " ".join("-p %s" % x for x in ports) if ports else ""
    hostname = "-h %s" % socket.gethostname() if socket.gethostname() else ""
    cmd = ("docker run -d -i -t {hostname} {ports} {mounts} {image} "
           "/bin/bash -c '" + user_create_cmd() +
           "bcbio_nextgen.py {bcbio_nextgen_cl}"
           "\"'")
    process = subprocess.Popen(cmd.format(**locals()), shell=True, stdout=subprocess.PIPE)
    cid = process.communicate()[0].strip()
    try:
        do.run("docker attach -nostdin %s" % cid, "Running in docker container: %s" % cid,
               log_stdout=True)
    except:
        print("Stopping docker container")
        subprocess.call("docker kill %s" % cid, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    finally:
        subprocess.call("docker kill %s" % cid, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        subprocess.call("docker rm %s" % cid, shell=True, stdout=subprocess.PIPE)
    return cid

def user_create_cmd(chown_cmd=""):
    """Create a user on the docker container with equivalent UID/GIDs to external user.
    """
    user = pwd.getpwuid(os.getuid())
    group = grp.getgrgid(os.getgid())
    container_bcbio_dir = "/usr/local/share"
    homedir = "/home/{user.pw_name}".format(**locals())
    cmd = ("addgroup --quiet --gid {group.gr_gid} {group.gr_name}; "
           "useradd -m -d {homedir} -s /bin/bash -g {group.gr_gid} -o -u {user.pw_uid} {user.pw_name}; "
           + chown_cmd +
           "su - -s /bin/bash {user.pw_name} -c \"cd {homedir} && "
           + proxy_cmd())
    return cmd.format(**locals())

def proxy_cmd():
    """Pass external proxy information inside container for retrieval.
    """
    out = "git config --global url.https://github.com/.insteadOf git://github.com/ && "
    for proxyenv in ["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy",
                     "ALL_PROXY", "all_proxy", "FTP_PROXY", "ftp_proxy",
                     "RSYNC_PROXY", "rsync_proxy"]:
        if proxyenv in os.environ:
            out += "export %s=%s && " % (proxyenv, os.environ[proxyenv])
    return out
