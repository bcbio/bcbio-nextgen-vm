"""Install or upgrade a bcbio-nextgen installation.
"""
from __future__ import print_function
from __future__ import unicode_literals

import json
import os
import subprocess

import progressbar as pb
import requests

from bcbiovm.docker import manage, mounts

def full(args, dockerconf):
    """Full installaction of docker image and data.
    """
    if args.install_tools:
        pull(dockerconf)
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

def pull(dockerconf):
    """Pull down latest docker image, using export uploaded to S3 bucket.

    Long term plan is to use the docker index server but upload size is
    currently smaller with an exported gzipped image.
    """
    print("Retrieving bcbio-nextgen docker image with code and tools")
    #subprocess.check_call(["docker", "pull", image])
    dl_image = os.path.basename(dockerconf["image_url"])
    response = requests.get(dockerconf["image_url"], stream=True)
    size = int(response.headers['Content-Length'].strip())
    widgets = [dl_image, pb.Percentage(), ' ', pb.Bar(),
               ' ', pb.ETA(), ' ', pb.FileTransferSpeed()]
    pbar = pb.ProgressBar(widgets=widgets, maxval=size).start()
    transferred_size = 0
    with open(dl_image, "wb") as out_handle:
        for buf in response.iter_content(1024):
            if buf:
                out_handle.write(buf)
                transferred_size += len(buf)
                pbar.update(transferred_size)
    pbar.finish()
    del response
    subprocess.check_call("gzip -dc %s | docker import - %s" % (dl_image, dockerconf["image"]),
                          shell=True)
    os.remove(dl_image)
