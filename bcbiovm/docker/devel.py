"""Utilities to help with developing using bcbio inside of docker.
"""
import os
import subprocess

from bcbio.provenance import do
from bcbiovm.docker import install

def setup_cmd(subparsers):
    parser = subparsers.add_parser("devel", help="Utilities to help with develping using bcbion inside of docker")
    psub = parser.add_subparsers(title="[devel commands]")

    iparser = psub.add_parser("setup_install", help="Run a python setup.py install inside of the current directory")
    iparser.add_argument("-i", "--image", help="Image name to write updates to",
                         default=install.DEFAULT_IMAGE)
    iparser.set_defaults(func=_run_setup_install)

def _run_setup_install(args):
    """Install python code from a bcbio-nextgen development tree inside of docker.
    """
    mounts = ["-v", "%s:%s" % (os.getcwd(), "/tmp/bcbio-nextgen")]
    cmd = ["docker", "run", "-i", "-d"] + mounts + [args.image] + \
          ["bash", "-l", "-c",
           ("rm -rf /usr/local/share/bcbio-nextgen/anaconda/lib/python2.7/site-packages/bcbio && "
            "cd /tmp/bcbio-nextgen && "
            "/usr/local/share/bcbio-nextgen/anaconda/bin/python setup.py install")]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    cid = process.communicate()[0].strip()
    do.run(["docker", "attach", "--no-stdin", cid], "Running in docker container: %s" % cid,
           log_stdout=True)
    subprocess.check_call(["docker", "commit", cid, args.image])
    subprocess.check_call(["docker", "rm", cid], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print("Updated bcbio-nextgen install in docker container: %s" % args.image)
