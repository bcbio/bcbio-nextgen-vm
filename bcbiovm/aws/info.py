"""Reports status of bcbio instances you're launched on AWS.
"""
from __future__ import print_function

import boto.ec2
import boto.iam
import boto.vpc

from common import ecluster_config


def bootstrap(args):
    cluster_config = ecluster_config(args.econfig, args.cluster)

    _iam_info()
    _sg_info(cluster_config)
    _vpc_info(cluster_config)
    print()
    _instance_info(cluster_config)


def _iam_info():
    conn = boto.iam.connection.IAMConnection()

    users = conn.get_all_users()
    users = users[u'list_users_response'][u'list_users_result']['users']
    if not users:
        print("WARNING: no IAM users exist.")
        return

    expect_iam_username = "bcbio"

    if any([user['user_name'] == expect_iam_username for user in users]):
        print("OK: expected IAM user '{}' exists.".format(expect_iam_username))
    else:
        print("WARNING: IAM user '{}' does not exist.".format(
            expect_iam_username))


def _sg_info(cluster_config):
    conn = boto.ec2.connect_to_region(cluster_config['cloud']['ec2_region'])

    security_groups = conn.get_all_security_groups()
    if not security_groups:
        print("WARNING: no security groups defined.")
        return

    expected_sg_name = cluster_config['cluster']['security_group']

    if any([sg.name == expected_sg_name for sg in security_groups]):
        print("OK: expected security group '{}' exists.".format(
            expected_sg_name))
    else:
        print("WARNING: security group '{}' does not exist.".format(
            expected_sg_name))


def _vpc_info(cluster_config):
    conn = boto.vpc.VPCConnection()

    vpcs = conn.get_all_vpcs()
    if not vpcs:
        print("WARNING: no VPCs exist.")
        return

    expected_vpc_name = cluster_config['cloud']['vpc']

    if any([vpc.tags['Name'] == expected_vpc_name for vpc in vpcs]):
        print("OK: VPC '{}' exists.".format(expected_vpc_name))
    else:
        print("WARNING: VPC '{}' does not exist.".format(expected_vpc_name))


def _instance_info(cluster_config):
    conn = boto.vpc.VPCConnection()
    vpcs = conn.get_all_vpcs()

    vpcs_by_id = {}
    for vpc in vpcs:
        vpcs_by_id[vpc.id] = vpc.tags['Name']

    conn = boto.ec2.connect_to_region(cluster_config['cloud']['ec2_region'])

    reservations = conn.get_all_reservations()
    if not reservations:
        print("WARNING: no instances.")
        return

    vpc_name = cluster_config['cloud']['vpc']

    print("Instances in VPC '{}':".format(vpc_name))
    for res in reservations:
        for inst in res.instances:
            if vpcs_by_id.get(inst.vpc_id) != vpc_name:
                continue
            print("\tname: {} ({}, {}) in {}".format(
                inst.tags.get("Name", "(none)"), inst.instance_type,
                inst.state, inst.placement))
