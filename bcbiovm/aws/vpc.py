"""Create a VPC and associated AWS resources for running bcbio on AWS.
"""
from __future__ import print_function

import os
import re
import sys

import boto.ec2

from bcbiovm.aws import common


def bootstrap(args):
    _setup_vpc(args)
    print("Created VPC: %s" % args.cluster)


def _setup_vpc(args):
    cidr_regex = r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/\d{1,2}$'
    if not re.search(cidr_regex, args.network):
        sys.stderr.write(
            'Network {} is not in CIDR (a.b.c.d/e) format.\n'.format(
                args.network))
        sys.exit(1)

    net, mask = args.network.split('/')
    if int(mask) > 23:
        sys.stderr.write('Network must be at least a /23 in size.\n')
        sys.exit(1)
    compute_subnet = '{}/24'.format(net)

    cluster_config = common.ecluster_config(args.econfig, args.cluster)
    conn = boto.connect_vpc(
        aws_access_key_id=cluster_config['cloud']['ec2_access_key'],
        aws_secret_access_key=cluster_config['cloud']['ec2_secret_key'])

    existing_vpcs = conn.get_all_vpcs(filters={'tag:Name': args.cluster})
    if existing_vpcs:
        if args.recreate:
            # FIXME: this doesn't automatically remove resources in the VPC
            # like the AWS management console does.
            conn.delete_vpc(existing_vpcs[0].id)
        else:
            raise Exception('VPC {} already exists.'.format(args.cluster))

    vpc = conn.create_vpc(args.network)
    vpc.add_tag('Name', args.cluster)

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

    subnet = conn.create_subnet(vpc.id, compute_subnet)
    subnet.add_tag('Name', '{}_cluster'.format(args.cluster))
    conn.associate_route_table(rtb.id, subnet.id)
