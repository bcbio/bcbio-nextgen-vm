"""Bootstrap AWS to enable simple up/down of bcbio with Ansible scripts.
"""
from __future__ import print_function

import argparse
import datetime
import os
import shutil
import string

import requests
import yaml

from bcbiovm.aws import iam, vpc

def setup_cmd(awsparser):
    parser = awsparser.add_parser("ansible",
                                  help="Create AWS resources for running ansible scripts",
                                  formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("zone", help="AWS availability zone to create resources in (ie. us-east-1a)")
    parser.add_argument("--keypair", help="Create new keypair for access",
                        dest="keypair", action="store_true", default=False)
    parser.add_argument("-n", "--network", default="10.0.0.0/16",
                        help="network to use for the VPC, "
                             "in CIDR notation (a.b.c.d/e)")
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="base name for created VPC resources")
    parser.set_defaults(func=create_resources)

def _zone_to_region(zone):
    region = zone[:]
    while region[-1] in string.lowercase:
        region = region[:-1]
    return region

def create_resources(args):
    out_file = "project_vars.yaml"
    if os.path.exists(out_file):
        bak_file = out_file + ".bak%s" % datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        shutil.move(out_file, bak_file)
    keypair_info = _setup_keypair(args)
    vpc_info = vpc.setup_vpc(args, _zone_to_region(args.zone))
    out = [("instance_type", "t2.small"),
           ("spot_price", "null"),
           ("volume", "vol-CHANGEME"),
           ("keypair", keypair_info["user_key_name"]),
           ("image_id", _lookup_image_by_region(_zone_to_region(args.zone))),
           ("vpc_subnet", vpc_info["subnet_id"]),
           ("iam_role", _create_iam_role(args)),
           ("security_group", vpc_info["security_group"]),
           ("region", _zone_to_region(args.zone)),
           ("zone", args.zone)]
    with open(out_file, "w") as out_handle:
        for k, v in out:
            out_handle.write("%s: %s\n" % (k, v))
    print("AWS resources setup and written to %s" % out_file)
    print("Edit instance_type, spot_price (if desired) and volume before running")
    if keypair_info.get("user_key_private"):
        print("Setup keypair %s, use private key %s" % (keypair_info["user_key_name"],
                                                        keypair_info["user_key_private"]))

def _lookup_image_by_region(region):
    """Retrieve Ubuntu AMI for the given region from official table.
    """
    url = "http://cloud-images.ubuntu.com/locator/ec2/releasesTable"
    r = requests.get(url)
    r_parsed = yaml.load(r.text)
    available = []
    for (cur_region, _, version, _, instance_type, _, ami_id, _) in r_parsed["aaData"]:
        if cur_region == region and instance_type == "hvm:ebs-ssd" and "LTS" in version:
            ami_id = ami_id.split(">")[1].split("<")[0]
            available.append((version, ami_id))
    available.sort(reverse=True)
    return available[0][1]

def _create_iam_role(args):
    import boto.iam
    conn = boto.iam.connect_to_region(_zone_to_region(args.zone))
    return iam.bcbio_s3_instance_profile(conn, args)["instance_profile"]

def _setup_keypair(args):
    if not args.keypair:
        return {"user_key_name": args.cluster}
    else:
        return iam.create_keypair(region=_zone_to_region(args.zone), keyname=args.cluster)
