"""Create a VPC and associated resources for running bcbio on AWS.
"""
from __future__ import print_function

import re
import sys

import boto.ec2

from bcbiovm.aws import common


def bootstrap(args):
    _setup_placment_group(args)
    _setup_vpc(args)


def _setup_placment_group(args):
    cluster_config = common.ecluster_config(args.econfig, args.cluster)
    conn = boto.connect_vpc(
        aws_access_key_id=cluster_config['cloud']['ec2_access_key'],
        aws_secret_access_key=cluster_config['cloud']['ec2_secret_key'])

    pgname = "{}_cluster_pg".format(args.cluster)
    pgs = conn.get_all_placement_groups()
    if pgname not in [x.name for x in pgs]:
        conn.create_placement_group(pgname)
        print("Placement group %s created." % pgname)
    else:
        print("Placement group %s already exists. Skipping" % pgname)


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
            # vpc.detele() alone doesn't automatically remove its
            # dependency as the AWS management console does.
            # So, we delete dependencies before deleting the vpc
            # itself.

            # collect ids for each
            vpc_id = existing_vpcs[0].id

            subnet_id = None
            rte_table_id = None
            igw_id = None
            sg_id = None
            sg_obj = None

            # is there a subnet associated with this vpc?
            for subnet in conn.get_all_subnets():
                if vpc_id == subnet.vpc_id:
                    subnet_id = subnet.id

            # are there a routing tables to delete?
            for rte_table in conn.get_all_route_tables():
                if vpc_id == rte_table.vpc_id:
                    rte_table_id = rte_table.id

            # is there a gateway to delete?
            for igw in conn.get_all_internet_gateways():
                if igw.tags[u'Name'] == u'bcbio_gw':
                    igw_id = igw.id

            # is there a security group to delete?
            for sg in conn.get_all_security_groups():
                if vpc_id == sg.vpc_id:
                    sg_id = sg.id
                    sg_obj = sg

            # delete subnet
            conn.delete_subnet(subnet_id)
            # delete route
            conn.delete_route(rte_table_id, '0.0.0.0/0')
            # delete route table
            conn.delete_route_table(rte_table_id)
            # detach gateway for vpc
            conn.detach_internet_gateway(igw_id, vpc_id)
            # delete gateway
            conn.delete_internet_gateway(igw_id)
            # delete security group
            sg_obj.delete()
            conn.delete_vpc(vpc_id)

        else:
            print('VPC {} already exists. Skipping. Use --recreate to re-create if needed.'.format(args.cluster))
            return

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

    print("Created VPC: %s" % args.cluster)
