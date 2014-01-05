"""Install or upgrade a bcbio-nextgen installation.
"""
from __future__ import print_function
from __future__ import unicode_literals

import json
import subprocess

import requests

from bcbiovm.docker import manage, mounts

def full(args, dockerconf):
    """Full installaction of docker image and data.
    """
    if args.install_tools:
        pull(dockerconf["image"])
    success = True
    dmounts = mounts.prepare_system(args.datadir, dockerconf["biodata_dir"])
    with manage.bcbio_docker(dockerconf, dmounts, args) as cid:
        print("Running data installation with docker container: %s" % cid)
        r = install_data(args, dockerconf["port"])
        if r is None or r.status_code != 200:
            success = False
            print("Problem installing data. For detailed logs, run:\n"
                  "docker logs {0}".format(cid))
    if success:
        print("bcbio-nextgen successfully upgraded")

def install_data(args, port):
    payload = json.dumps({"genomes": args.genomes, "aligners": args.aligners,
                          "install_data": args.install_data})
    try:
        return requests.get("http://localhost:{port}/install".format(port=port), params={"args": payload})
    except requests.exceptions.ConnectionError:
        return None

def pull(image):
    print("Retrieving bcbio-nextgen docker images with code and tools")
    subprocess.check_call(["docker", "pull", image])
