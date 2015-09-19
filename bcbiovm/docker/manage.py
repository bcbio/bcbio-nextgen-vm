"""Manage stopping and starting a docker container for running analysis.
"""
from __future__ import print_function
import grp
import operator
import os
import platform
import pwd
import subprocess

from bcbio.log import logger
from bcbio.provenance import do

def run_bcbio_cmd(image, mounts, bcbio_nextgen_args, ports=None):
    """Run command in docker container with the supplied arguments to bcbio-nextgen.py.
    """
    mounts = reduce(operator.add, (["-v", m] for m in list(set(mounts))), [])
    ports = reduce(operator.add, (["-p", p] for p in ports or []), [])
    envs = _get_pass_envs()
    envs += ["-e", "PERL5LIB=/usr/local/lib/perl5"]
    networking = ["--net=host"]  # Use host-networking so Docker works correctly on AWS VPCs

    user = pwd.getpwuid(os.getuid())
    group = grp.getgrgid(os.getgid())

    cmd = ["docker", "run", "-d", "-i"] + networking + ports + mounts + envs + [image]
    # On Mac OSX boot2docker runs the docker server inside VirtualBox, which maps
    # the root user there to the external user. In this case we want to run the job
    # as root so it will have permission to access user directories. Since the Docker server
    # is sandboxed inside VirtualBox this doesn't have the same security worries as
    # on a Linux system.
    # On Linux systems, we run commands as the original calling user so they have the
    # same permissions inside the Docker container as they do externally.
    if platform.system() != "Darwin":
        cmd += ["/sbin/createsetuser", user.pw_name, str(user.pw_uid), group.gr_name, str(group.gr_gid)]
    cmd += ["bcbio_nextgen.py"] + bcbio_nextgen_args
    # logger.info(" ".join(cmd))
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    cid = process.communicate()[0].strip()
    try:
        do.run(["docker", "attach", "--no-stdin", cid], "Running in docker container: %s" % cid,
               log_stdout=True)
    except subprocess.CalledProcessError as e:
        print("Stopping docker container")
        subprocess.call(["docker", "kill", cid], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        raise e
    finally:
        subprocess.call(["docker", "kill", cid], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        subprocess.call(["docker", "rm", cid], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return cid

def _get_pass_envs():
    """Pass external proxy information inside container for retrieval.
    """
    out = []
    for proxyenv in ["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy",
                     "ALL_PROXY", "all_proxy", "FTP_PROXY", "ftp_proxy",
                     "RSYNC_PROXY", "rsync_proxy",
                     "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]:
        if proxyenv in os.environ:
            out += ["-e", "%s=%s" % (proxyenv, os.environ[proxyenv])]
    return out
