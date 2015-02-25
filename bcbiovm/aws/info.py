"""Reports status of bcbio instances you're launched on AWS.
"""
from __future__ import print_function

import boto.ec2
import boto.iam
import boto.vpc
import toolz as tz

from bcbiovm.aws import common

def setup_cmd(awsparser):
    parser = awsparser.add_parser("info", help="Information on existing AWS clusters")
    parser.set_defaults(func=print_info)
    common.add_default_ec_args(parser)

def print_info(args):
    all_cc = common.ecluster_config(args.econfig)
    print("Available clusters: %s" % ",".join(all_cc.cluster_conf.keys()))
    print()
    print("Configuration for cluster '%s':" % (args.cluster))
    cluster_config = common.ecluster_config(args.econfig, args.cluster)
    _cluster_info(cluster_config)
    print()
    print("AWS setup:")
    _iam_info()
    _sg_info(cluster_config)
    _vpc_info(cluster_config)
    print()
    _instance_info(cluster_config)

def _cluster_info(config):
    """Provide high level details about the setup of the current cluster.
    """
    compute_c = tz.get_in(["nodes", "compute"], config)
    frontend_c = tz.get_in(["nodes", "frontend"], config)
    print(" Frontend: %s with %sGb NFS storage" % (frontend_c["flavor"], frontend_c["encrypted_volume_size"]))
    if int(compute_c.get("compute_nodes", 0)) > 0:
        print(" Cluster: %s %s machines" % (compute_c["compute_nodes"], compute_c["flavor"]))

def _iam_info():
    conn = boto.iam.connection.IAMConnection()

    users = conn.get_all_users()
    users = users[u'list_users_response'][u'list_users_result']['users']
    if not users:
        print("WARNING: no IAM users exist.")
        return

    expect_iam_username = "bcbio"

    if any([user['user_name'] == expect_iam_username for user in users]):
        print(" OK: expected IAM user '{}' exists.".format(expect_iam_username))
    else:
        print(" WARNING: IAM user '{}' does not exist.".format(
            expect_iam_username))


def _sg_info(cluster_config):
    conn = boto.ec2.connect_to_region(cluster_config['cloud']['ec2_region'])

    security_groups = conn.get_all_security_groups()
    if not security_groups:
        print(" WARNING: no security groups defined.")
        return

    expected_sg_name = cluster_config['cluster']['security_group']

    if any([sg.name == expected_sg_name for sg in security_groups]):
        print(" OK: expected security group '{}' exists.".format(
            expected_sg_name))
    else:
        print(" WARNING: security group '{}' does not exist.".format(
            expected_sg_name))


def _vpc_info(cluster_config):
    conn = boto.vpc.VPCConnection()

    vpcs = conn.get_all_vpcs()
    if not vpcs:
        print(" WARNING: no VPCs exist.")
        return

    expected_vpc_name = cluster_config['cloud']['vpc']

    if any([vpc.tags.get('Name', "") == expected_vpc_name for vpc in vpcs]):
        print(" OK: VPC '{}' exists.".format(expected_vpc_name))
    else:
        print(" WARNING: VPC '{}' does not exist.".format(expected_vpc_name))


def _instance_info(cluster_config):
    conn = boto.vpc.VPCConnection()
    vpcs = conn.get_all_vpcs()

    vpcs_by_id = {}
    for vpc in vpcs:
        vpcs_by_id[vpc.id] = vpc.tags.get('Name', "")

    conn = boto.ec2.connect_to_region(cluster_config['cloud']['ec2_region'])

    reservations = conn.get_all_reservations()
    vpc_name = cluster_config['cloud']['vpc']

    print("Instances in VPC '{}':".format(vpc_name))
    for res in reservations:
        for inst in res.instances:
            if vpcs_by_id.get(inst.vpc_id) != vpc_name:
                continue

            ip_address = inst.ip_address
            if not inst.ip_address:
                ip_address = inst.private_ip_address
            print("\t{} ({}, {}) at {} in {}".format(
                inst.tags.get("Name", "(none)"), inst.instance_type,
                inst.state, ip_address, inst.placement))
