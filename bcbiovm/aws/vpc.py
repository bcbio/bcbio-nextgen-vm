"""Create a VPC and associated resources for running bcbio on AWS.
"""
from __future__ import print_function

import re
import sys

import boto.ec2

from bcbiovm.aws import common


def bootstrap(args):
    vpc_info = setup_vpc(args)
    _setup_placment_group(args, vpc_info)

def _setup_placment_group(args, vpc_info):
    cluster_config = common.ecluster_config(args.econfig, args.cluster)
    conn = boto.connect_vpc(
        aws_access_key_id=cluster_config['cloud']['ec2_access_key'],
        aws_secret_access_key=cluster_config['cloud']['ec2_secret_key'])

    pgname = "{}_cluster_pg".format(args.cluster)
    pgs = conn.get_all_placement_groups()
    if vpc_info.get("created") or pgname not in [x.name for x in pgs]:
        if pgname in [x.name for x in pgs]:
            print("Refreshing placement group %s." % pgname)
            conn.delete_placement_group(pgname)
        conn.create_placement_group(pgname)
        print("Placement group %s created." % pgname)
    else:
        print("Placement group %s already exists. Skipping" % pgname)

def setup_vpc(args, region=None):
    cidr_regex = r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/\d{1,2}$'
    if not re.search(cidr_regex, args.network):
        raise ValueError(
            'Network {} is not in CIDR (a.b.c.d/e) format.\n'.format(
                args.network))

    net, mask = args.network.split('/')
    if int(mask) > 23:
        sys.stderr.write('Network must be at least a /23 in size.\n')
        sys.exit(1)
    compute_subnet = '{}/24'.format(net)

    if hasattr(args, "econfig"):
        cluster_config = common.ecluster_config(args.econfig, args.cluster)
        conn = boto.connect_vpc(
            aws_access_key_id=cluster_config['cloud']['ec2_access_key'],
            aws_secret_access_key=cluster_config['cloud']['ec2_secret_key'])
    else:
        assert region is not None, "Require region for setting up VPC from scratch"
        ec2_conn = boto.ec2.connect_to_region(region)
        conn = boto.connect_vpc(region=ec2_conn.region)

    out = {"vpc": args.cluster,
           "security_group": "%s_cluster_sg" % args.cluster}
    azone = args.zone if hasattr(args, "zone") else None
    existing_vpcs = conn.get_all_vpcs(filters={'tag:Name': args.cluster})
    if existing_vpcs:
        if hasattr(args, "recreate") and args.recreate:
            raise NotImplementedError("bcbio does not currently remove VPCs. "
                                      "The easiest way is to do this manually in the console: "
                                      "https://console.aws.amazon.com/vpc/home")
            # FIXME: this doesn't automatically remove resources in the VPC
            # like the AWS management console does.
            conn.delete_vpc(existing_vpcs[0].id)
        else:
            print('VPC {} already exists. Skipping creation.'.format(args.cluster))
            out["created"] = False
            out["subnet_id"] = _get_subnet_id(existing_vpcs[0], conn, azone)
            return out

    vpc = conn.create_vpc(args.network)
    while vpc.state != "available":
        vpc.update()
    vpc.add_tag('Name', args.cluster)
    conn.modify_vpc_attribute(vpc.id, enable_dns_support=True)
    conn.modify_vpc_attribute(vpc.id, enable_dns_hostnames=True)
    out["created"] = True

    sg = conn.create_security_group(
      '{}_cluster_sg'.format(args.cluster),
      'bcbio cluster nodes', vpc.id)
    sg.authorize(ip_protocol='tcp', from_port=22, to_port=22,
                 cidr_ip='0.0.0.0/0')
    sg.authorize(ip_protocol='-1', src_group=sg)

    igw = conn.create_internet_gateway()
    igw.add_tag('Name', '{}_gw'.format(args.cluster))
    conn.attach_internet_gateway(igw.id, vpc.id)

    rtb = conn.create_route_table(vpc.id)
    rtb.add_tag('Name', '{}_rtable'.format(args.cluster))
    conn.create_route(rtb.id, '0.0.0.0/0', igw.id)

    subnet = conn.create_subnet(vpc.id, compute_subnet, availability_zone=azone)
    subnet.add_tag('Name', '{}_cluster'.format(args.cluster))
    conn.associate_route_table(rtb.id, subnet.id)

    print("Created VPC: %s" % args.cluster)
    out["subnet_id"] = _get_subnet_id(vpc, conn, azone)
    return out

def _get_subnet_id(vpc, conn, zone=None):
    filters = {"vpcId": str(vpc.id)}
    if zone:
        filters["availabilityZone"] = zone
    subnets = conn.get_all_subnets(filters=filters)
    if subnets:
        return subnets[0].id
    else:
        raise ValueError("Could not find correct subnet in VPC %s" % (vpc.id))
