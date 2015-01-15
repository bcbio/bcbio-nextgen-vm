"""Create an Intel ICEL stack on AWS.

https://wiki.hpdd.intel.com/display/PUB/Intel+Cloud+Edition+for+Lustre*+-+Global+Support+HVM
"""
from __future__ import print_function

import argparse
import getpass
import json
import os
import re
import socket
import struct
import sys
import time

import boto.cloudformation
import boto.ec2
import boto.s3
import elasticluster
import requests

from bcbiovm.aws import common


ICEL_TEMPLATES = {
    'ap-northeast-1': 'http://s3-ap-northeast-1.amazonaws.com/hpdd-templates-ap-northeast-1/gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template',
    'ap-southeast-1': 'http://s3-ap-southeast-1.amazonaws.com/hpdd-templates-ap-southeast-1/gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template',
    'ap-southeast-2': 'http://s3-ap-southeast-2.amazonaws.com/hpdd-templates-ap-southeast-2/gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template',
    'eu-west-1': 'http://s3-eu-west-1.amazonaws.com/hpdd-templates-eu-west-1/gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template',
    'sa-east-1': 'http://s3-sa-east-1.amazonaws.com/hpdd-templates-sa-east-1/gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template',
    'us-east-1': 'http://s3.amazonaws.com/hpdd-templates-us-east-1/gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template',
    'us-west-1': 'http://s3-us-west-1.amazonaws.com/hpdd-templates-us-west-1/gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template',
    'us-west-2': 'http://s3-us-west-2.amazonaws.com/hpdd-templates-us-west-2/gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template',
}


def setup_cmd(awsparser):
    parser_c = awsparser.add_parser("icel",
                                    help="Create scratch filesystem using Intel Cloud Edition for Lustre")
    icel_parser = parser_c.add_subparsers(title="[icel create]")

    # ## Create

    parser = icel_parser.add_parser("create",
                                    help="Create scratch filesystem using Intel Cloud Edition for Lustre",
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--econfig", help="Elasticluster bcbio configuration file",
                        default=common.DEFAULT_EC_CONFIG)
    parser.add_argument("--recreate", action="store_true", default=False,
                        help="Remove and recreate the stack, "
                             "destroying all data stored on it")
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Emit verbose output when running "
                             "Ansible playbooks")

    parser.add_argument("-s", "--size", type=int, default="2048",
                        help="Size of the Lustre filesystem, in gigabytes")
    parser.add_argument("-o", "--oss-count", type=int, default="4",
                        help="Number of OSS nodes")
    parser.add_argument("-l", "--lun-count", type=int, default="4",
                        help="Number of EBS LUNs per OSS")

    parser.add_argument("-n", "--network", metavar="NETWORK", dest="network",
                        help="Network (in CIDR notation, a.b.c.d/e) to "
                             "place Lustre servers in")

    parser.add_argument("-b", "--bucket", default="bcbio-lustre-%s" % getpass.getuser(),
                        help="bucket to store generated ICEL template for CloudFormation")
    parser.add_argument(metavar="STACK_NAME", dest="stack_name", nargs="?",
                        default="bcbiolustre",
                        help="CloudFormation name for the new stack")
    parser.set_defaults(func=create)

    # ## Spec

    parser = icel_parser.add_parser("fs_spec",
                                    help="Get the filesystem spec for a running filesystem",
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--econfig", help="Elasticluster bcbio configuration file",
                        default=common.DEFAULT_EC_CONFIG)
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")
    parser.add_argument(metavar="STACK_NAME", dest="stack_name", nargs="?",
                        default="bcbiolustre",
                        help="CloudFormation name for the stack")
    parser.set_defaults(func=fs_spec)

    # ## Mount

    parser = icel_parser.add_parser("mount",
                                    help="Mount Lustre filesystem on all cluster nodes",
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--econfig", help="Elasticluster bcbio configuration file",
                        default=common.DEFAULT_EC_CONFIG)
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Emit verbose output when running "
                             "Ansible playbooks")
    parser.add_argument(metavar="STACK_NAME", dest="stack_name", nargs="?",
                        default="bcbiolustre",
                        help="CloudFormation name for the new stack")
    parser.set_defaults(func=mount)

    # ## Unmount

    parser = icel_parser.add_parser("unmount",
                                    help="Unmount Lustre filesystem on all cluster nodes",
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--econfig", help="Elasticluster bcbio configuration file",
                        default=common.DEFAULT_EC_CONFIG)
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Emit verbose output when running "
                             "Ansible playbooks")
    parser.add_argument(metavar="STACK_NAME", dest="stack_name", nargs="?",
                        default="bcbiolustre",
                        help="CloudFormation name for the new stack")
    parser.set_defaults(func=unmount)

    # ## Stop

    parser = icel_parser.add_parser("stop",
                                    help="Stop the running Lustre filesystem and clean up resources",
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--econfig", help="Elasticluster bcbio configuration file",
                        default=common.DEFAULT_EC_CONFIG)
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")
    parser.add_argument(metavar="STACK_NAME", dest="stack_name", nargs="?",
                        default="bcbiolustre",
                        help="CloudFormation name for the new stack")
    parser.set_defaults(func=stop)


def create(args):
    if args.network:
        cidr_regex = r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/\d{1,2}$'
        if not re.search(cidr_regex, args.network):
            sys.stderr.write(
                'Network {} is not in CIDR (a.b.c.d/e) format.\n'.format(
                    args.network))
            sys.exit(1)

    config = common.ecluster_config(args.econfig)
    cluster_config = config.cluster_conf[args.cluster]
    try:
        cluster = config.load_cluster(args.cluster)
        cluster_storage_path = cluster.repository.storage_path
    except elasticluster.exceptions.ClusterNotFound:
        # Assume the default storage path if the cluster doesn't exist,
        # so we can start an ICEL stack in parallel with cluster startup.
        cluster_storage_path = elasticluster.conf.Configurator.default_storage_dir

    icel_param = {
        'oss_count': args.oss_count,
        'ost_vol_size': args.size / args.oss_count / args.lun_count,
        'ost_vol_count': args.lun_count,
    }
    template_url = _upload_icel_cf_template(
        icel_param, args.bucket, cluster_config['cloud'])

    _create_stack(
        args.stack_name, template_url, args.network,
        args.cluster, cluster_config, args.recreate)
    try:
        sys.stdout.write('Waiting for stack to launch (this will take '
                         'a few minutes)')
        sys.stdout.flush()
        _wait_for_stack(args.stack_name, 'CREATE_COMPLETE',
                        15 * 60, cluster_config['cloud'])
    except Exception as e:
        sys.stderr.write('{}\n'.format(str(e)))
        sys.exit(1)

    ssh_config_path = os.path.join(
        cluster_storage_path, 'icel-{}.ssh_config'.format(args.stack_name))
    _write_ssh_config(ssh_config_path, args.stack_name, cluster_config)

    ansible_config_path = os.path.join(
        cluster_storage_path,
        'icel-{}.ansible_config'.format(args.stack_name))
    _write_ansible_config(
        ansible_config_path, args.stack_name, cluster_storage_path)

    inventory_path = os.path.join(
        cluster_storage_path,
        'icel-{}.inventory'.format(args.stack_name))
    _write_inventory(inventory_path, args.stack_name, cluster_config['cloud'])

    playbook_path = os.path.join(
        common.ANSIBLE_BASE, "roles", "icel", "tasks", "main.yml")
    common.run_ansible_pb(
        inventory_path, playbook_path, args, ansible_cfg=ansible_config_path)


def fs_spec(args):
    cluster_config = common.ecluster_config(args.econfig, args.cluster)
    print(_get_fs_spec(args.stack_name, cluster_config['cloud']))


def mount(args):
    mount_or_unmount(args, True)


def unmount(args):
    mount_or_unmount(args, False)


def mount_or_unmount(args, mount=True):
    cluster = common.ecluster_config(args.econfig).load_cluster(args.cluster)

    inventory_path = os.path.join(
        cluster.repository.storage_path,
        'ansible-inventory.{}'.format(args.cluster))

    if mount:
        playbook_file = "mount.yml"
    else:
        playbook_file = "unmount.yml"
    playbook_path = os.path.join(
        common.ANSIBLE_BASE, "roles", "lustre_client", "tasks", playbook_file)

    def get_lustre_vars(args, cluster_config):
        return {'lustre_fs_spec': _get_fs_spec(
            args.stack_name, cluster_config['cloud'])}

    common.run_ansible_pb(
        inventory_path, playbook_path, args, get_lustre_vars)


def _template_param(tree, param):
    return [
        (i, name)
        for i, name
         in enumerate(tree)
         if type(name) in (str, unicode) and
            name.startswith(param)
    ][0]


def _upload_icel_cf_template(param, bucket_name, aws_config):
    url = ICEL_TEMPLATES[aws_config['ec2_region']]
    source_template = requests.get(url)
    tree = json.loads(source_template.text)
    tree['Description'] = tree['Description'].replace(
        '4 Object Storage Servers',
        '{} Object Storage Servers'.format(param['oss_count']))
    if aws_config["ec2_region"] == "us-east-1":
        tree["Parameters"]["NATInstanceType"]["AllowedValues"].append("m3.medium")
        tree["Mappings"]["AWSNATAMI"]["us-east-1"]["AMI"] = "ami-184dc970"
    resources = tree['Resources']

    # We don't need the demo Lustre client instance.
    del resources['ClientInstanceProfile']
    del resources['ClientLaunchConfig']
    del resources['ClientNodes']
    del resources['ClientRole']
    resources['BasePolicy']['Properties']['Roles'] = [
        item
        for item
         in resources['BasePolicy']['Properties']['Roles']
         if item['Ref'] != 'ClientRole'
    ]

    for section in ['MDS', 'MDS', 'MGS']:
        cf_params = resources['{}LaunchConfig'.format(section)]['Metadata']['AWS::CloudFormation::Init']['config']['files']['/etc/loci.conf']['content']['Fn::Join'][1]

        index = _template_param(cf_params, 'OssCount:')[0]
        cf_params[index + 1] = param['oss_count']

        index = _template_param(cf_params, 'OstVolumeCount:')[0]
        cf_params[index + 1] = param['ost_vol_count']

        index = _template_param(cf_params, 'OstVolumeSize:')[0]
        cf_params[index + 1] = param['ost_vol_size']

    resources['OSSNodes']['Properties']['DesiredCapacity'] = param['oss_count']
    resources['OSSNodes']['Properties']['MaxSize'] = param['oss_count']
    resources['OSSNodes']['Properties']['MinSize'] = param['oss_count']
    resources['OssWaitCondition']['Properties']['Count'] = param['oss_count']

    conn = boto.s3.connect_to_region(
        aws_config['ec2_region'],
        aws_access_key_id=aws_config['ec2_access_key'],
        aws_secret_access_key=aws_config['ec2_secret_key'])

    bucket = conn.create_bucket(bucket_name)

    k = boto.s3.key.Key(bucket)
    k.key = 'icel-cf-template.json'
    k.set_contents_from_string(json.dumps(tree))
    k.make_public()

    return k.generate_url(5 * 60, query_auth=False)

def stop(args):
    cluster_config = common.ecluster_config(args.econfig, args.cluster)
    _delete_stack(args.stack_name, cluster_config)

def _delete_stack(stack_name, cluster_config):
    """Delete a Lustre CloudFormation stack.
    """
    cf_conn = boto.cloudformation.connect_to_region(
        cluster_config['cloud']['ec2_region'],
        aws_access_key_id=cluster_config['cloud']['ec2_access_key'],
        aws_secret_access_key=cluster_config['cloud']['ec2_secret_key'])
    cf_conn.delete_stack(stack_name)
    sys.stdout.write('Waiting for stack to delete (this will take a few minutes)')
    sys.stdout.flush()
    _wait_for_stack(stack_name, 'DELETE_COMPLETE',
                    15 * 60, cluster_config['cloud'])

# The awscli(1) equivalent of this is:
#
# aws cloudformation create-stack --stack-name STACK_NAME \
#   --template-url TEMPLATE_URL \
#   --capabilities CAPABILITY_IAM \
#   --parameters \
#       ParameterKey=FsName,ParameterValue=scratch \
#       ParameterKey=AccessFrom,ParameterValue=0.0.0.0/0 \
#       ParameterKey=VpcId,ParameterValue=vpc-c0ffee \
#       ParameterKey=VpcPrivateCIDR,ParameterValue=a.b.c.d/e \
#       ParameterKey=VpcPublicSubnetId,ParameterValue=subnet-deadbeef \
#       ParameterKey=KeyName,ParameterValue=keypair@example.com \
#       ParameterKey=HTTPFrom,ParameterValue=0.0.0.0/0 \
#       ParameterKey=SSHFrom,ParameterValue=0.0.0.0/0
def _create_stack(stack_name, template_url, lustre_net, cluster,
                  cluster_config, recreate):
    conn = boto.connect_vpc(
        aws_access_key_id=cluster_config['cloud']['ec2_access_key'],
        aws_secret_access_key=cluster_config['cloud']['ec2_secret_key'])

    cf_conn = boto.cloudformation.connect_to_region(
        cluster_config['cloud']['ec2_region'],
        aws_access_key_id=cluster_config['cloud']['ec2_access_key'],
        aws_secret_access_key=cluster_config['cloud']['ec2_secret_key'])

    for stack in cf_conn.list_stacks('CREATE_COMPLETE'):
        if stack.stack_name == stack_name:
            if recreate:
                _delete_stack(stack_name, cluster_config)
            else:
                raise Exception('Stack {} already exists.'.format(stack_name))

    for vpc in conn.get_all_vpcs():
        if cluster_config['cloud']['vpc'] in [vpc.tags.get('Name'), vpc.id]:
            break
    else:
        raise Exception('Elasticluster must be running in '
                        'an AWS VPC to start an ICEL stack.')

    public_subnet_name = '{}_cluster'.format(cluster)
    public_subnets = conn.get_all_subnets(
        filters={'vpcId': vpc.id, 'tag:Name': public_subnet_name})
    if len(public_subnets) > 1:
        raise Exception(
            'More than one subnet named {} exists in VPC {}/{}'.format(
                public_subnet_name, vpc.id, vpc.tags.get('Name')))
    if len(public_subnets) == 0:
        raise Exception(
            'A subnet named {} does not exist in VPC {}/{}'.format(
                public_subnet_name, vpc.id, vpc.tags.get('Name')))
    public_subnet = public_subnets[0]

    if not lustre_net:
        vpc_net = vpc.cidr_block.split('/')[0]
        vpc_net_int = struct.unpack('>L', socket.inet_aton(vpc_net))[0]
        lustre_net = socket.inet_ntoa(struct.pack('>L', vpc_net_int + 256))
        lustre_net = '{}/24'.format(lustre_net)

    aws_config = cluster_config["cloud"]
    cf_conn.create_stack(stack_name,
        template_url=template_url,
        capabilities=['CAPABILITY_IAM'],
        parameters=(
            ('FsName', 'scratch'),
            ('AccessFrom', vpc.cidr_block),
            ('NATInstanceType', "m3.medium" if aws_config["ec2_region"] == "us-east-1" else "m1.small"),
            ('VpcId', vpc.id),
            ('VpcPrivateCIDR', lustre_net),
            ('VpcPublicSubnetId', public_subnet.id),
            ('KeyName', cluster_config['login']['user_key_name']),
            ('HTTPFrom', '0.0.0.0/0'),
            ('SSHFrom', '0.0.0.0/0'),
        ))


def _wait_for_stack(stack_name, desired_state, wait_for, aws_config):
    conn = boto.cloudformation.connect_to_region(
        aws_config['ec2_region'],
        aws_access_key_id=aws_config['ec2_access_key'],
        aws_secret_access_key=aws_config['ec2_secret_key'])

    stack = conn.describe_stacks(stack_name)[0]

    interval_length = 10
    for interval in xrange(wait_for / interval_length):
        stack.update()
        status = stack.stack_status

        if status == desired_state:
            print()
            return
        elif status.endswith('_IN_PROGRESS'):
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(interval_length)
            continue
        else:
            failed_events = [
                event
                for event
                 in stack.describe_events()
                 if event.resource_status.endswith('_FAILED')
            ]
            failed_descr = ','.join([
                '{}: {}'.format(
                    event.logical_resource_id, event.resource_status_reason)
                for event
                 in failed_events
            ])
            print()
            raise Exception(
                'Stack {} did not launch successfully: {}: {}'.format(
                stack_name, status, failed_descr))
    print()


def _get_stack_param(stack_name, param_name, aws_config):
    conn = boto.cloudformation.connect_to_region(
        aws_config['ec2_region'],
        aws_access_key_id=aws_config['ec2_access_key'],
        aws_secret_access_key=aws_config['ec2_secret_key'])

    icel_stack = conn.describe_stacks(stack_name)[0]
    return [
        param.value
        for param
         in icel_stack.parameters
         if param.key == param_name
    ]


def get_stack_name(node_addr, aws_config):
    """Get the name of the CloudFormation stack a node belongs to."""
    conn = boto.ec2.connect_to_region(
        aws_config['ec2_region'],
        aws_access_key_id=aws_config['ec2_access_key'],
        aws_secret_access_key=aws_config['ec2_secret_key'])

    reservations = conn.get_all_reservations()
    for resv in reservations:
        for inst in resv.instances:
            # Non-HA MGTs don't have a tagged interface.
            if inst.private_ip_address == node_addr:
                return inst.tags['aws:cloudformation:stack-name']

            for iface in inst.interfaces:
                iface.update()
                if iface.private_ip_address == node_addr:
                    return inst.tags.get('aws:cloudformation:stack-name')


def get_instances(stack_name, aws_config):
    """Get the IP addresses of all instances in a CloudFormation stack."""
    conn = boto.ec2.connect_to_region(
        aws_config['ec2_region'],
        aws_access_key_id=aws_config['ec2_access_key'],
        aws_secret_access_key=aws_config['ec2_secret_key'])

    reservations = conn.get_all_reservations(
        filters={
            'tag:aws:cloudformation:stack-name': stack_name,
        }
    )
    addrs = {}
    for resv in reservations:
        for inst in resv.instances:
            # Instances might still be around for stopped stacks with
            # the same stack name, so ignore them.
            if inst.state in ['terminated', 'shutting-down']:
                continue

            if inst.tags['Name'] == 'NATDevice':
                addrs[inst.tags['Name']] = inst.ip_address
            else:
                addrs[inst.tags['Name']] = inst.private_ip_address

    return addrs


def _get_mgt_ip_addr(stack_name, aws_config):
    conn = boto.ec2.connect_to_region(
        aws_config['ec2_region'],
        aws_access_key_id=aws_config['ec2_access_key'],
        aws_secret_access_key=aws_config['ec2_secret_key'])

    reservations = conn.get_all_reservations(
        filters={
            'tag:Name': 'mgt*',
            'tag:aws:cloudformation:stack-name': stack_name,
        }
    )
    for resv in reservations:
        for inst in resv.instances:
            for iface in inst.interfaces:
                iface.update()
                if iface.tags.get('lustre:server_role') == 'mgt':
                    # HA MGTs have a tagged interface.
                    return iface.private_ip_address

            # Non-HA MGTs don't.
            return inst.private_ip_address

    return None


def _get_fs_spec(stack_name, aws_config):
    mgt_ipaddr = _get_mgt_ip_addr(stack_name, aws_config)
    fs_name = _get_stack_param(stack_name, 'FsName', aws_config)[0]
    return '{}:/{}'.format(mgt_ipaddr, fs_name)


def _write_ssh_config(path, stack_name, cluster_config):
    template_path = os.path.join(
        common.ANSIBLE_BASE, "ssh_config-icel.template")
    with open(template_path) as input:
        ssh_template = input.read()

    instances = get_instances(stack_name, cluster_config['cloud'])
    formatted = ssh_template.format(
        nat_device_ipaddr=instances['NATDevice'],
        user_key_private=cluster_config['login']['user_key_private'],
    )

    with open(path, 'w') as output:
        output.write(formatted)


def _write_inventory(path, stack_name, aws_config):
    instances = get_instances(stack_name, aws_config)

    with open(path, 'w') as inventory:
        inventory.write('[mgs]\n')
        mgts = [name for name in instances if name.startswith('mgt')]
        for name in mgts:
            inventory.write(
                '{} ansible_ssh_host={} ansible_ssh_user=ec2-user\n'.format(
                    name, instances[name]))

        inventory.write('\n')
        inventory.write('[mds]\n')
        mdts = [name for name in instances if name.startswith('mdt')]
        for name in mdts:
            inventory.write(
                '{} ansible_ssh_host={} ansible_ssh_user=ec2-user\n'.format(
                    name, instances[name]))

        inventory.write('\n')
        inventory.write('[oss]\n')
        osts = [name for name in instances if name.startswith('ost')]
        for name in osts:
            inventory.write(
                '{} ansible_ssh_host={} ansible_ssh_user=ec2-user\n'.format(
                    name, instances[name]))


def _write_ansible_config(path, stack_name, storage_path):
    template_path = os.path.join(
        common.ANSIBLE_BASE, "ansible-icel.cfg.template")
    with open(template_path) as input:
        ssh_template = input.read()

    formatted = ssh_template.format(
        cluster_storage_path=storage_path, stack_name=stack_name)

    with open(path, 'w') as output:
        output.write(formatted)
