"""Manage stopping and starting a docker container for running analysis.
"""
from __future__ import print_function
from __future__ import unicode_literals
import contextlib
import grp
import os
import pwd
import subprocess
import time

import requests

@contextlib.contextmanager
def bcbio_docker(dconf, mounts, args):
    """Provide a running bcbio-nextgen docker server with automatic stop on completion.
    """
    cid = None
    if args.develrepo:
        yield start_devel(dconf["image"], args.port, dconf["port"], mounts,
                          dconf["biodata_dir"], args.develrepo)
    else:
        try:
            cid = start(dconf["image"], args.port, dconf["port"], mounts, dconf["biodata_dir"])
            wait(dconf["port"])
            yield cid
        finally:
            if cid:
                stop(cid)

def start_devel(image, hport, cport, mounts, docker_biodata_dir, repo):
    """Start a docker container for development, attached to the provided code repo.
    Uses a standard name 'bcbio-develrepo' to avoid launching multiple images on
    re-run.
    """
    name = "bcbio-develrepo"
    # look for existing running processes
    process = subprocess.Popen(["docker", "ps"], stdout=subprocess.PIPE)
    containers, _ = process.communicate()
    for line in containers.split("\n"):
        if line.find(name) >= 0:
            return line.split()[0]
    # start a new container running bash for development
    mounts.append("%s:/tmp/bcbio-nextgen" % repo)
    mounts = " ".join("-v %s" % x for x in mounts)
    chown_cmd = ("chown -R {user.pw_name} /usr/local/share/bcbio-nextgen/anaconda/lib/python2.7/site-packages && "
                 "chown -R {user.pw_name} /usr/local/share/bcbio-nextgen/anaconda/bin && ")
    cmd = ("docker run -d -i -t -name {name} -p {hport}:{cport} {mounts} {image} "
           "/bin/bash -c '"
           + user_create_cmd(chown_cmd) +
           "/bin/bash"
           "\"'")
    process = subprocess.Popen(cmd.format(**locals()), shell=True, stdout=subprocess.PIPE)
    cid, _ = process.communicate()
    return cid.rstrip()

def start(image, hport, cport, mounts, docker_biodata_dir):
    mounts = " ".join("-v %s" % x for x in mounts)
    cmd = ("docker run -d -p {hport}:{cport} {mounts} {image} "
           "/bin/bash -c '" + user_create_cmd() +
           "bcbio_nextgen.py server --port={cport} --biodata_dir={docker_biodata_dir}"
           "\"'")
    process = subprocess.Popen(cmd.format(**locals()), shell=True, stdout=subprocess.PIPE)
    cid, _ = process.communicate()
    return cid.rstrip()

def user_create_cmd(chown_cmd=""):
    """Create a user on the docker container with equivalent UID/GIDs to external user.
    """
    user = pwd.getpwuid(os.getuid())
    group = grp.getgrgid(os.getgid())
    container_bcbio_dir = "/usr/local/share"
    homedir = "/home/{user.pw_name}".format(**locals())
    cmd = ("addgroup --gid {group.gr_gid} {group.gr_name} && "
           "useradd -m -d {homedir} -g {group.gr_gid} -o -u {user.pw_uid} {user.pw_name} && "
           + chown_cmd +
           "su - -s /bin/bash {user.pw_name} -c \"cd {homedir} && ")
    return cmd.format(**locals())

def wait(port):
    """Wait for server to start.
    """
    num_tries = 0
    max_tries = 40
    while 1:
        try:
            requests.get("http://localhost:{port}/status".format(**locals()),
                         params={"run_id": "checkup"})
            break
        except requests.exceptions.ConnectionError:
            if num_tries > max_tries:
                raise
            else:
                num_tries += 1
                time.sleep(1)

def stop(cid):
    subprocess.check_call(["docker", "kill", cid], stdout=subprocess.PIPE)
    #subprocess.check_call(["docker", "rm", cid], stdout=subprocess.PIPE)
