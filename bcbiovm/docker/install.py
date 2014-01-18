"""Install or upgrade a bcbio-nextgen installation.
"""
from __future__ import print_function
from __future__ import unicode_literals

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
    dmounts = mounts.prepare_system(args.datadir, dockerconf["biodata_dir"])
    cmd = manage.docker_cmd(dockerconf["image"], dmounts, _get_cl(args))
    subprocess.call(cmd, shell=True)

def _get_cl(args):
    clargs = ["upgrade"]
    if args.install_data:
        clargs.append("--data")
    for g in args.genomes:
        clargs.extend(["--genomes", g])
    for a in args.aligners:
        clargs.extend(["--aligners", a])
    return " ".join(clargs)

def pull(dockerconf):
    """Pull down latest docker image, using export uploaded to S3 bucket.

    Long term plan is to use the docker index server but upload size is
    currently smaller with an exported gzipped image.
    """
    print("Retrieving bcbio-nextgen docker image with code and tools")
    #subprocess.check_call(["docker", "pull", image])
    dl_image = os.path.basename(dockerconf["image_url"])
    response = requests.get(dockerconf["image_url"], stream=True)
    size = int(response.headers.get("Content-Length", "0").strip())
    if size:
        widgets = [dl_image, pb.Percentage(), ' ', pb.Bar(),
                   ' ', pb.ETA(), ' ', pb.FileTransferSpeed()]
        pbar = pb.ProgressBar(widgets=widgets, maxval=size).start()
    transferred_size = 0
    with open(dl_image, "wb") as out_handle:
        for buf in response.iter_content(1024):
            if buf:
                out_handle.write(buf)
                transferred_size += len(buf)
                if size:
                    pbar.update(transferred_size)
    if size:
        pbar.finish()
    del response
    subprocess.check_call("gzip -dc %s | docker import - %s" % (dl_image, dockerconf["image"]),
                          shell=True)
    os.remove(dl_image)
