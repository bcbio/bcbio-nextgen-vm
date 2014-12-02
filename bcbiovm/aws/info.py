"""Reports status of bcbio instances you're launched on AWS. 
"""
from __future__ import print_function

import sys

import boto.ec2
import boto.iam
import boto.vpc

from bcbiovm.aws import common

'''This script can help you understand if your bcbio instances on AWS
is setup correct or troubleshoot it if there is a problem.
'''


def bootstrap(args):
    conn = boto.ec2.connect_to_region("us-east-1")

    _iam_info()
    _sg_info(conn)
    _vpc_info()
    _instance_info(conn)


def _vpc_info():
    print('\nVPC check: Bcbio setups a VPC.')
    conn = boto.vpc.VPCConnection()
    for vpn in conn.get_all_vpcs():
        print('\t{0}'.format(vpn))


def _iam_info():
    print (
        '''
IAM check: Bcbio needs an IAM user called 'bcbio'. Do you see
it in this list? ''')

    conn = boto.iam.connection.IAMConnection()

    # user
    user_list = conn.get_all_users()
    user_list = user_list[u'list_users_response'][u'list_users_result']['users']
    if user_list:
        for u in user_list:
            print("\tIAM User: {0}".format(u['user_name']))
    else:
        print("\nIAM User: None.")


def _sg_info(conn):
    print(
        '''
Security group check: Bcbio needs a security group called
'bcbio_cluster_sg'. Do you see it in this list?''')

    # list security groups.
    group_list = conn.get_all_security_groups()
    if group_list:
        for sg in group_list:
            print('\t{0}'.format(sg))
    else:
        print("no security groups.")


def _instance_info(conn):
    print('\nHere is a list of instances. ')
    res_list = conn.get_all_reservations()
    if res_list:
        for res in res_list:
            for inst in res.instances:
                print("\ttype: {0}, zone: {1}".format(
                    inst.instance_type, inst.placement))
    else:
        print("\tNo instances")
