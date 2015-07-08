"""Create an Intel ICEL stack on AWS.

More information regarding ICEL can be found on the following link:
https://goo.gl/jxEj0e
"""
import os
import re
import time

import boto.cloudformation
import boto.ec2
import boto.s3
import json
import struct
import socket
import requests
import toolz
from elasticluster import exceptions as ec_exc


from bcbiovm.common import cluster as cluster_ops
from bcbiovm.common import constant
from bcbiovm.common import exception
from bcbiovm.common import utils
from bcbiovm.provider import playbook as provider_playbook

LOG = utils.get_logger(__name__)


class ICELOps(object):

    """Create an Intel ICEL stack on AWS."""

    DELETE_COMPLETE = 'DELETE_COMPLETE'
    CREATE_COMPLETE = 'CREATE_COMPLETE'
    TEMPLATES = {
        'ap-northeast-1': ('http://s3-ap-northeast-1.amazonaws.com/'
                           'hpdd-templates-ap-northeast-1/gs/1.0.1/'
                           'hpdd-gs-ha-c3-small-1.0.1.template'),
        'ap-southeast-1': ('http://s3-ap-southeast-1.amazonaws.com/'
                           'hpdd-templates-ap-southeast-1/gs/1.0.1/'
                           'hpdd-gs-ha-c3-small-1.0.1.template'),
        'ap-southeast-2': ('http://s3-ap-southeast-2.amazonaws.com/'
                           'hpdd-templates-ap-southeast-2/gs/1.0.1/'
                           'hpdd-gs-ha-c3-small-1.0.1.template'),
        'eu-west-1': ('http://s3-eu-west-1.amazonaws.com/hpdd-templates-eu-'
                      'west-1/gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template'),
        'sa-east-1': ('http://s3-sa-east-1.amazonaws.com/hpdd-templates-sa-'
                      'east-1 /gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template'),
        'us-east-1': ('http://s3.amazonaws.com/hpdd-templates-us-east-1/gs/'
                      '1.0.1/hpdd-gs-ha-c3-small-1.0.1.template'),
        'us-west-1': ('http://s3-us-west-1.amazonaws.com/hpdd-templates-us-'
                      'west-1 /gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template'),
        'us-west-2': ('http://s3-us-west-2.amazonaws.com/hpdd-templates-us-'
                      'west-2/gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template'),
    }

    def __init__(self, cluster, config):
        self._ecluster = cluster_ops.ElastiCluster(constant.PROVIDER.AWS)
        self._ecluster.load_config(config)
        self._cluster_config = self._ecluster.get_config(cluster)

        self._cluster_name = cluster
        self._config_path = config

    @staticmethod
    def _check_network(network):
        """Check if the received network is valid."""
        if not network:
            return

        cidr_regex = r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/\d{1,2}$'
        if not re.search(cidr_regex, network):
            raise ValueError(
                'Network %(network)s is not in CIDR (a.b.c.d/e) format.' %
                {"network": network})

    @staticmethod
    def _get_network(network, vpc):
        """Get the network in CIDR format."""
        if not network:
            vpc_net = vpc.cidr_block.split('/')[0]
            vpc_net_int = struct.unpack('>L', socket.inet_aton(vpc_net))[0]
            network = socket.inet_ntoa(struct.pack('>L', vpc_net_int + 256))
            network = '{}/24'.format(network)

        return network

    @staticmethod
    def _get_flavor(region):
        """Get the recommended instance type for the received region."""
        if region == "us-east-1":
            return "m3.medium"

        return "m1.small"

    def _get_storage_path(self):
        """Return the storage path for the current cluster."""
        try:
            cluster = self._ecluster.get_cluster(self._cluster_name)
            cluster_storage_path = cluster.repository.storage_path
        except ec_exc.ClusterNotFound:
            # Assume the default storage path if the cluster doesn't exist,
            # so we can start an ICEL stack in parallel with cluster startup.
            cluster_storage_path = os.path.join(
                os.path.dirname(self._config_path), "storage")
            if not os.path.exists(cluster_storage_path):
                os.makedirs(cluster_storage_path)

        return cluster_storage_path

    @staticmethod
    def _get_index(container, prefix):
        """Return the index and the name of the first item which starts
        with the received prefix.
        """
        for index, name in enumerate(container):
            if isinstance(name, (str, unicode)) and name.startswith(prefix):
                return (index, name)

    def _upload_icel_cf_template(self, oss_count, ost_vol_size, ost_vol_count,
                                 bucket_name):
        """Upload the ICEL CloudFormation template file."""
        # pylint: disable=too-many-locals

        aws_config = self._cluster_config['cloud']
        icel_template = self.TEMPLATES[aws_config['ec2_region']]

        try:
            source_template = requests.get(icel_template)
        except requests.exceptions.RequestException:
            LOG.exception("HTTP request failed: %(url)s",
                          {"url": icel_template})
            return

        tree = source_template.json()
        tree['Description'] = tree['Description'].replace(
            '4 Object Storage Servers',
            '{} Object Storage Servers'.format(oss_count))

        if aws_config["ec2_region"] == "us-east-1":
            tree["Parameters"]["NATInstanceType"]["AllowedValues"].append(
                "m3.medium")
            tree["Mappings"]["AWSNATAMI"]["us-east-1"]["AMI"] = "ami-184dc970"
        resources = tree['Resources']

        # We don't need the demo Lustre client instance.
        for section in ('ClientInstanceProfile', 'ClientLaunchConfig',
                        'ClientNodes', 'ClientRole'):
            resources.pop(section)

        for item in resources['BasePolicy']['Properties']['Roles'][:]:
            if item['Ref'] == 'ClientRole':
                resources['BasePolicy']['Properties']['Roles'].remove(item)

        for section_name in ['MDS', 'MDS', 'MGS']:
            section = resources['{}LaunchConfig'.format(section_name)]
            cf_params = toolz.get_in(['Metadata', 'AWS::CloudFormation::Init',
                                      'config', 'files', '/etc/loci.conf',
                                      'content', 'Fn::Join'], section)[1]

            index, _ = self._get_index(cf_params, 'OssCount:')
            cf_params[index + 1] = oss_count

            index, _ = self._get_index(cf_params, 'OstVolumeCount:')
            cf_params[index + 1] = ost_vol_count

            index, _ = self._get_index(cf_params, 'OstVolumeSize:')
            cf_params[index + 1] = ost_vol_size

        resources['OSSNodes']['Properties']['DesiredCapacity'] = oss_count
        resources['OSSNodes']['Properties']['MaxSize'] = oss_count
        resources['OSSNodes']['Properties']['MinSize'] = oss_count
        resources['OssWaitCondition']['Properties']['Count'] = oss_count

        conn = boto.s3.connect_to_region(
            aws_config['ec2_region'],
            aws_access_key_id=aws_config['ec2_access_key'],
            aws_secret_access_key=aws_config['ec2_secret_key'])
        bucket = conn.create_bucket(bucket_name)

        s3_key = boto.s3.key.Key(bucket)
        s3_key.key = 'icel-cf-template.json'
        s3_key.set_contents_from_string(json.dumps(tree))
        s3_key.make_public()

        return s3_key.generate_url(5 * 60, query_auth=False)

    def _get_public_subnet(self, connection, vpc):
        """Return the public subnet if it is available."""
        public_subnet_name = '{}_cluster'.format(self._cluster_name)
        public_subnets = connection.get_all_subnets(
            filters={'vpcId': vpc.id, 'tag:Name': public_subnet_name})

        if len(public_subnets) > 1:
            raise exception.BCBioException(
                "More than one subnet named %(subnet_name)s exists in VPC "
                "%(vpc_id)s/%(vpc_name)s", subnet_name=public_subnet_name,
                vpc_id=vpc.id, vpc_name=vpc.tags.get('Name'))

        if len(public_subnets) == 0:
            raise exception.BCBioException(
                "A subnet named  %(subnet_name)s does not exists in VPC "
                "%(vpc_id)s/%(vpc_name)s", subnet_name=public_subnet_name,
                vpc_id=vpc.id, vpc_name=vpc.tags.get('Name'))

        return public_subnets[0]

    def _get_vpc(self, connection):
        """Return the VPC if it available."""
        required_vpc = self._cluster_config['cloud']['vpc']
        for vpc in connection.get_all_vpcs():
            if required_vpc in (vpc.tags.get('Name'), vpc.id):
                return vpc

        raise exception.BCBioException("Elasticluster must be running in an "
                                       "AWS VPC to start an ICEL stack.")

    def _get_mgt_ip_addr(self, stack_name):
        """Return the ip address for Management Target.

        The MGT stores file system configuration information for use by
        the clients and other Lustre components.
        """
        aws_config = self._cluster_config["cloud"]
        connection = boto.ec2.connect_to_region(
            aws_config['ec2_region'],
            aws_access_key_id=aws_config['ec2_access_key'],
            aws_secret_access_key=aws_config['ec2_secret_key'])

        reservations = connection.get_all_reservations(
            filters={'tag:Name': 'mgt*',
                     'tag:aws:cloudformation:stack-name': stack_name})

        for resv in reservations:
            for instance in resv.instances:
                for iface in instance.interfaces:
                    iface.update()
                    if iface.tags.get('lustre:server_role') == 'mgt':
                        # HA MGTs have a tagged interface.
                        return iface.private_ip_address

                # Non-HA MGTs don't.
                return instance.private_ip_address

        return None

    def _get_stack_param(self, stack_name, parameter):
        """Return the value of the received parameter from the stack."""
        aws_config = self._cluster_config["cloud"]
        # NOTE(alenxadrucoman): I have changed boto.ec2 with
        #                       boto.cloudformation.
        # Instance of 'EC2Connection' has no 'describe_stacks'
        # member (no-member)
        connection = boto.cloudformation.connect_to_region(
            aws_config['ec2_region'],
            aws_access_key_id=aws_config['ec2_access_key'],
            aws_secret_access_key=aws_config['ec2_secret_key'])

        icel_stack = connection.describe_stacks(stack_name)[0]
        for param in icel_stack.parameters:
            if param.key == parameter:
                return param.value

    def _get_fs_spec(self, stack_name):
        """Return the filesystem specifications."""
        mgt_ipaddr = self._get_mgt_ip_addr(stack_name)
        fs_name = self._get_stack_param(stack_name, 'FsName')
        return '{}:/{}'.format(mgt_ipaddr, fs_name)

    @staticmethod
    def _write_ansible_config(path, stack_name, storage_path):
        """Create the ansible config file."""
        template_path = constant.PATH.ANSIBLE_TEMPLATE
        with open(template_path, 'r') as file_handle:
            ansible_template = file_handle.read()

        content = ansible_template.format(cluster_storage_path=storage_path,
                                          stack_name=stack_name)
        utils.write_file(path, content, open_mode="w")

    def _write_inventory(self, path, stack_name):
        """Create the inventory file."""
        instances = self.instances(stack_name)
        section = {'mgt': [], 'mdt': [], 'ost': []}

        for name, ip_address in instances.items():
            if name[:3] in section:
                continue
            section[name[:3]].append('{} ansible_ssh_host={} '
                                     'ansible_ssh_user=ec2-user'
                                     .format(name, ip_address))
        content = ['[mgs]']
        content.extend(section['mgt'])
        content.append('\n[mds]')
        content.extend(section['mdt'])
        content.append('\n[oss]')
        content.extend(section['ost'])

        utils.write_file(path, '\n'.join(content), open_mode="w")

    def _write_ssh_config(self, path, stack_name):
        """Create the ssh config file."""
        template_path = constant.PATH.SSH_TEMPLATE
        with open(template_path, 'r') as file_handle:
            ssh_template = file_handle.read()

        instances = self.instances(stack_name)
        content = ssh_template.format(
            nat_device_ipaddr=instances['NATDevice'],
            user_key_private=self._cluster_config['login']['user_key_private'],
        )
        utils.write_file(path, content, open_mode="w")

    def _wait_for_stack(self, stack_name, desired_state, timeout,
                        retry_interval=10):
        """Wait until the desired state is reached."""
        aws_config = self._cluster_config['cloud']
        conn = boto.cloudformation.connect_to_region(
            aws_config['ec2_region'],
            aws_access_key_id=aws_config['ec2_access_key'],
            aws_secret_access_key=aws_config['ec2_secret_key'])

        stack = conn.describe_stacks(stack_name)[0]
        while timeout > 0:
            wait_time = min(retry_interval, timeout)

            stack.update()
            status = stack.stack_status
            if status == desired_state:
                return
            elif status.endswith('_IN_PROGRESS'):
                pass
            else:
                failed_events = []
                for event in stack.describe_events():
                    if event.resource_status.endswith('_FAILED'):
                        failed_events.append(
                            "%(id)s: %(status)s" %
                            {"id": event.logical_resource_id,
                             "status": event.resource_status_reason})

                raise exception.BCBioException(
                    "Stack %(stack)s did not launch successfully: "
                    "%(status)s: %(failed)s", stack=stack_name, status=status,
                    failed=",".join(failed_events))

            time.sleep(wait_time)
            timeout -= retry_interval

    def _delete_stack(self, stack_name):
        """Delete a Lustre CloudFormation stack."""
        cluster_config = self._cluster_config['cloud']
        cf_conn = boto.cloudformation.connect_to_region(
            cluster_config['ec2_region'],
            aws_access_key_id=cluster_config['ec2_access_key'],
            aws_secret_access_key=cluster_config['ec2_secret_key'])
        cf_conn.delete_stack(stack_name)
        LOG.info('Waiting for stack to delete (this will take a few minutes)')
        self._wait_for_stack(stack_name=stack_name,
                             desired_state=self.DELETE_COMPLETE,
                             timeout=900,   # 15 minutes
                             retry_interval=10)

    def _mount(self, stack_name, mount=True):
        """Mount or unmount Lustre filesystem on all cluster nodes."""
        cluster = self._ecluster.get_cluster(self._cluster_name)
        inventory_path = os.path.join(
            cluster.repository.storage_path,
            'ansible-inventory.{}'.format(self._cluster_name))
        aws_playbook = provider_playbook.AWSPlaybook()
        if mount:
            playbook_path = aws_playbook.mount_lustre
        else:
            playbook_path = aws_playbook.unmount_lustre

        # pylint: disable=unused-argument
        def get_lustre_vars(cluster_config):
            """Extra variables to inject into a playbook."""
            return {'lustre_fs_spec': self._get_fs_spec(stack_name)}

        playbook = cluster_ops.AnsiblePlaybook(
            inventory_path=inventory_path,
            playbook_path=playbook_path,
            config=self._config_path,
            cluster=self._cluster_name,
            extra_vars=get_lustre_vars,
            provider=constant.PROVIDER.AWS)
        return playbook.run()

    def stack_name(self, node_addr):
        """Get the name of the CloudFormation stack a node belongs to."""
        aws_config = self._cluster_config["cloud"]
        connection = boto.ec2.connect_to_region(
            aws_config['ec2_region'],
            aws_access_key_id=aws_config['ec2_access_key'],
            aws_secret_access_key=aws_config['ec2_secret_key'])

        reservations = connection.get_all_reservations()
        for resv in reservations:
            for inst in resv.instances:
                # Non-HA MGTs don't have a tagged interface.
                if inst.private_ip_address == node_addr:
                    return inst.tags['aws:cloudformation:stack-name']

                for iface in inst.interfaces:
                    iface.update()
                    if iface.private_ip_address == node_addr:
                        return inst.tags.get('aws:cloudformation:stack-name')

    def instances(self, stack_name):
        """Get the IP addresses of all instances in a CloudFormation stack."""
        aws_config = self._cluster_config["cloud"]
        conn = boto.ec2.connect_to_region(
            aws_config['ec2_region'],
            aws_access_key_id=aws_config['ec2_access_key'],
            aws_secret_access_key=aws_config['ec2_secret_key'])
        reservations = conn.get_all_reservations(
            filters={'tag:aws:cloudformation:stack-name': stack_name}
        )

        ip_address = {}
        for resv in reservations:
            for instance in resv.instances:
                if instance.state in ['terminated', 'shutting-down']:
                    # Instances might still be around for stopped stacks with
                    # the same stack name, so ignore them.
                    continue
                name = instance.tags['Name']
                if instance.tags['Name'] == 'NATDevice':
                    ip_address[name] = instance.ip_address
                else:
                    ip_address[name] = instance.private_ip_address
        return ip_address

    def create_stack(self, stack_name, template_url, lustre_net, recreate):
        """Creates a stack as specified in the template.

        :param stack_name:      CloudFormation name for the new stack
        :param template_url:    the url for icel template
        :param lustre_net:      network (in CIDR notation, a.b.c.d/e)
                                to place Lustre servers in
        :param recreate:        whether to remove and recreate the stack,
                                destroying all data stored on it

        After the call completes successfully, the stack creation starts.
        The awscli(1) equivalent of this is:

        aws cloudformation create-stack --stack-name STACK_NAME \
            --template-url TEMPLATE_URL \
            --capabilities CAPABILITY_IAM \
            --parameters \
                ParameterKey=FsName,ParameterValue=scratch \
                ParameterKey=AccessFrom,ParameterValue=0.0.0.0/0 \
                ParameterKey=VpcId,ParameterValue=vpc-c0ffee \
                ParameterKey=VpcPrivateCIDR,ParameterValue=a.b.c.d/e \
                ParameterKey=VpcPublicSubnetId,ParameterValue=subnet-deadbeef \
                ParameterKey=KeyName,ParameterValue=keypair@example.com \
                ParameterKey=HTTPFrom,ParameterValue=0.0.0.0/0 \
                ParameterKey=SSHFrom,ParameterValue=0.0.0.0/0
        """
        cluster_config = self._ecluster.get_config(self._cluster_name)
        conn = boto.connect_vpc(
            aws_access_key_id=cluster_config['cloud']['ec2_access_key'],
            aws_secret_access_key=cluster_config['cloud']['ec2_secret_key'])
        cf_conn = boto.cloudformation.connect_to_region(
            cluster_config['cloud']['ec2_region'],
            aws_access_key_id=cluster_config['cloud']['ec2_access_key'],
            aws_secret_access_key=cluster_config['cloud']['ec2_secret_key'])

        for stack in cf_conn.list_stacks(self.CREATE_COMPLETE):
            if stack.stack_name == stack_name:
                if recreate:
                    self._delete_stack(stack_name)
                else:
                    raise exception.BCBioException(
                        'Stack %(stack)s already exists.', stack=stack_name)

        vpc = self._get_vpc(conn)
        public_subnet = self._get_public_subnet(conn, vpc)
        lustre_net = self._get_network(lustre_net, vpc)
        region = cluster_config["cloud"]["ec2_region"]

        cf_conn.create_stack(
            stack_name,
            template_url=template_url,
            capabilities=['CAPABILITY_IAM'],
            parameters=(
                ('FsName', 'scratch'),
                ('AccessFrom', vpc.cidr_block),
                ('NATInstanceType', self._get_flavor(region)),
                ('VpcId', vpc.id),
                ('VpcPrivateCIDR', lustre_net),
                ('VpcPublicSubnetId', public_subnet.id),
                ('KeyName', cluster_config['login']['user_key_name']),
                ('HTTPFrom', '0.0.0.0/0'),
                ('SSHFrom', '0.0.0.0/0'),
            ))

    def create(self, network, bucket, stack_name, **kwargs):
        """Create scratch filesystem using Intel Cloud Edition for Lustre

        :param network:     network (in CIDR notation, a.b.c.d/e)
                            to place Lustre servers in
        :param bucket:      bucket to store generated ICEL template
                            for CloudFormation
        :param stack_name:  CloudFormation name for the new stack
        :param size:        size of the Lustre filesystem, in gigabytes
        :param oss_count:   number of OSS node
        :param lun_count:   number of EBS LUNs per OSS
        :param recreate:    whether to remove and recreate the stack,
                            destroying all data stored on it
        :param setup:       whether to run again the configuration steps
        """
        # pylint: disable = too-many-locals

        size = kwargs.pop("size", 2048)
        oss_count = kwargs.pop("oss_count", 4)
        lun_count = kwargs.pop("lun_count", 4)
        recreate = kwargs.pop("recreate", False)
        setup = kwargs.pop("setup", False)

        self._check_network(network)
        cluster_storage_path = self._get_storage_path()

        if not setup:
            template_url = self._upload_icel_cf_template(
                oss_count=oss_count,
                ost_vol_size=size / oss_count / lun_count,
                ost_vol_count=lun_count,
                bucket_name=bucket)

            self.create_stack(stack_name=stack_name,
                              template_url=template_url,
                              lustre_net=network,
                              recreate=recreate)

            LOG.info("Waiting for stack to launch (this will take a "
                     "few minutes)")
            try:
                self._wait_for_stack(stack_name=stack_name,
                                     desired_state=self.CREATE_COMPLETE,
                                     timeout=900)
            except exception.BCBioException as exc:
                LOG.exception(exc)
                return

        ssh_config_path = os.path.join(cluster_storage_path,
                                       'icel-{}.ssh_config'.format(stack_name))
        self._write_ssh_config(ssh_config_path, stack_name)

        ansible_config_path = os.path.join(
            cluster_storage_path, 'icel-{}.ansible_config'.format(stack_name))
        self._write_ansible_config(ansible_config_path, stack_name,
                                   cluster_storage_path)

        inventory_path = os.path.join(
            cluster_storage_path, 'icel-{}.inventory'.format(stack_name))
        self._write_inventory(inventory_path, stack_name)

        aws_playbook = provider_playbook.AWSPlaybook()
        playbook = cluster_ops.AnsiblePlaybook(
            inventory_path=inventory_path,
            playbook_path=aws_playbook.icel,
            config=self._config_path,
            cluster=self._cluster_name,
            ansible_cfg=ansible_config_path,
            provider=constant.PROVIDER.AWS)
        return playbook.run()

    def mount(self, stack_name):
        """Mount Lustre filesystem on all cluster nodes.

        :param stack_name:  CloudFormation name for the new stack
        """
        return self._mount(stack_name=stack_name, mount=True)

    def unmount(self, stack_name):
        """Unmount Lustre filesystem on all cluster nodes.

        :param stack_name:  CloudFormation name for the new stack
        """
        return self._mount(stack_name=stack_name, mount=False)

    def stop(self, stack_name):
        """Stop the running Lustre filesystem and clean up resources.

        :param stack_name:  CloudFormation name for the new stack
        """
        return self._delete_stack(stack_name)

    def fs_spec(self, stack_name):
        """Get the filesystem spec for a running filesystem.

        :param stack_name:  CloudFormation name for the new stack
        """
        return self._get_fs_spec(stack_name)
