"""Install or upgrade a bcbio-nextgen installation.
"""
from __future__ import print_function

import os
import subprocess
import sys

import progressbar as pb
import requests
import yaml

from bcbiovm.docker import manage, mounts

DEFAULT_IMAGE = "chapmanb/bcbio-nextgen-devel"

def full(args, dockerconf):
    """Full installaction of docker image and data.
    """
    updates = []
    args = add_install_defaults(args)
    if args.wrapper:
        updates.append("wrapper scripts")
        upgrade_bcbio_vm()
    if args.install_tools:
        updates.append("bcbio-nextgen code and third party tools")
        pull(args, dockerconf)
    _check_docker_image(args)
    dmounts = mounts.prepare_system(args.datadir, dockerconf["biodata_dir"])
    if args.install_data:
        updates.append("biological data")
    manage.run_bcbio_cmd(args.image, dmounts, _get_cl(args))
    _save_install_defaults(args)
    if updates:
        print("\nbcbio-nextgen-vm updated with latest %s" % " and ".join(updates))

def _get_cl(args):
    clargs = ["upgrade"]
    if args.install_data:
        clargs.append("--data")
    for g in args.genomes:
        clargs.extend(["--genomes", g])
    for a in args.aligners:
        clargs.extend(["--aligners", a])
    return clargs

def upgrade_bcbio_vm():
    """Upgrade bcbio-nextgen-vm wrapper code.
    """
    conda_bin = os.path.join(os.path.dirname(os.path.realpath(sys.executable)), "conda")
    if not os.path.exists(conda_bin):
        print("Cannot update bcbio-nextgen-vm; not installed with conda")
    else:
        subprocess.check_call([conda_bin, "install", "--yes",
                               "-c", "https://conda.binstar.org/collections/chapmanb/bcbio",
                               "bcbio-nextgen-vm"])

def pull(args, dockerconf):
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
    assert args.image, "Unspecified image name for docker import"
    subprocess.check_call("gzip -dc %s | docker import - %s" % (dl_image, args.image),
                          shell=True)
    os.remove(dl_image)

def _save_install_defaults(args):
    """Save arguments passed to installation to be used on subsequent upgrades.
    Avoids needing to re-include genomes and aligners on command line.
    """
    install_config = _get_config_file(args)
    if install_config is None:
        return
    if os.path.exists(install_config):
        with open(install_config) as in_handle:
            cur_config = yaml.load(in_handle)
    else:
        cur_config = {}
    for attr in ["genomes", "aligners"]:
        if not cur_config.get(attr):
            cur_config[attr] = []
        for x in getattr(args, attr):
            if x not in cur_config[attr]:
                cur_config[attr].append(str(x))
    if args.image != DEFAULT_IMAGE and args.image:
        cur_config["image"] = args.image
    with open(install_config, "w") as out_handle:
        yaml.dump(cur_config, out_handle, default_flow_style=False, allow_unicode=False)

def _get_install_defaults(args):
    install_config = _get_config_file(args)
    if install_config and os.path.exists(install_config):
        with open(install_config) as in_handle:
            return yaml.load(in_handle)
    return {}

def _add_docker_defaults(args, default_args):
    if not hasattr(args, "image") or not args.image:
        if default_args.get("image") and not default_args.get("images") == "None":
            args.image = default_args["image"]
        else:
            args.image = DEFAULT_IMAGE
    return args

def add_install_defaults(args):
    """Add previously saved installation defaults to command line arguments.
    """
    default_args = _get_install_defaults(args)
    for attr in ["genomes", "aligners"]:
        for x in default_args.get(attr, []):
            new_val = getattr(args, attr)
            if x not in getattr(args, attr):
                new_val.append(x)
            setattr(args, attr, new_val)
    args = _add_docker_defaults(args, default_args)
    return args

def _check_docker_image(args):
    """Ensure docker image exists.
    """
    for image in subprocess.check_output(["docker", "images"]).split("\n"):
        parts = image.split()
        if len(parts) > 1 and parts[0] == args.image:
            return
    raise ValueError("Could not find docker image %s in local repository" % args.image)

def docker_image_arg(args):
    if not hasattr(args, "image") or not args.image:
        default_args = _get_install_defaults(args)
        args = _add_docker_defaults(args, default_args)
    _check_docker_image(args)
    return args

def _get_config_file(args):
    config_dir = os.path.join(args.datadir, "config")
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
    return os.path.join(config_dir, "install-params.yaml")
