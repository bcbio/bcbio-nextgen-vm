"""
Create a VPC and associated resources for running bcbio on AWS.
"""
import re

import boto.ec2

from bcbiovm.common import cluster as clusterops
from bcbiovm.common import utils

LOG = utils.get_logger(__name__)


class VirtualPrivateCloud(object):

    """Create and setup the Virtual Private Cloud."""

    _GATEWAY_TAG = "%(cluster)s_gw"
    _SUBNET_TAG = "%(cluster)s_cluster"
    _CLUSTER_SG = "%(cluster)s_cluster_sg"
    _RTABLE_TAG = "%(cluster)s_rtable"

    def __init__(self, cluster, config, network, recreate):
        """
        :param config:      elasticluster config file
        :param cluster:     cluster name
        :param network:     network to use for the VPC, in CIDR
                            notation (a.b.c.d/e)
        :param recreate:    whether to recreate the VPC if exists
        """
        self._cluster_name = cluster
        self._network = network
        self._recreate = recreate

        ecluster = clusterops.ElastiCluster(config)
        cluster_config = ecluster.get_config(cluster)

        self._key_id = cluster_config['cloud']['ec2_access_key']
        self._access_key = cluster_config['cloud']['ec2_secret_key']

        self._check_network()

    def _check_network(self):
        """Check if the received network is valid."""
        cidr_regex = r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/\d{1,2}$'
        if not re.search(cidr_regex, self._network):
            raise ValueError(
                'Network %(network)s is not in CIDR (a.b.c.d/e) format.' %
                {"network": self._network})

        netmask = int(self._network.split('/')[1])
        if netmask > 23:
            raise ValueError('Network must be at least a /23 in size.')

    def _recreate_vpc(self, connection):
        """Remove and recreate the VPC, destroying all AWS resources
        contained in it.
        """
        existing_vpcs = connection.get_all_vpcs(
            filters={'tag:Name': self._cluster_name})

        if not existing_vpcs:
            return

        raise NotImplementedError(
            "bcbio does not currently remove VPCs. "
            "The easiest way is to do this manually in the console: "
            "https://console.aws.amazon.com/vpc/home")

        # FIXME(chapmanb): This doesn't automatically remove resources
        #                  in the VPC like the AWS management console does.
        #                  connection.delete_vpc(existing_vpcs[0].id)

    def _create_security_group(self, connection, vpc):
        """Create the security group for bcbio."""
        name = ('%(cluster)s_cluster_security_group' %
                {"cluster": self._cluster_name})
        security_group = connection.create_security_group(
            name, 'bcbio cluster nodes', vpc.id)
        security_group.authorize(ip_protocol='tcp', from_port=22, to_port=22,
                                 cidr_ip='0.0.0.0/0')
        security_group.authorize(ip_protocol='-1', src_group=security_group)

    def _create_network(self, connection, vpc):
        """Create and setup the network for the VPC."""
        gw_tag = "%(cluster)s_gw" % {"cluster": self._cluster_name}
        rt_tag = "%(cluster)s_rtable" % {"cluster": self._cluster_name}
        subnet_tag = "%(cluster)s_cluster" % {"cluster": self._cluster_name}
        compute_subnet = '%(net)s/24' % {"net": self._network.split('/')[0]}

        internet_gateway = connection.create_internet_gateway()
        internet_gateway.add_tag('Name', gw_tag)
        connection.attach_internet_gateway(internet_gateway.id, vpc.id)

        route_table = connection.create_route_table(vpc.id)
        route_table.add_tag('Name', rt_tag)
        connection.create_route(route_table.id, '0.0.0.0/0',
                                internet_gateway.id)

        subnet = connection.create_subnet(vpc.id, compute_subnet)
        subnet.add_tag('Name', subnet_tag)
        connection.associate_route_table(route_table.id, subnet.id)

    def _setup_placement_group(self, connection):
        """Setup the placement group for the current VPC."""
        name = "%(cluster)s_cluster_pg" % {"cluster": self._cluster_name}
        placement_groups = connection.get_all_placement_groups()

        if name in [pgroup.name for pgroup in placement_groups]:
            LOG.info("Refreshing placement group %(name)s.", {"name": name})
            connection.delete_placement_group(name)

        connection.create_placement_group(name)
        LOG.info("Placement group %(name)s created.", {"name": name})

    def run(self):
        """Create and setup the Virtual Private Cloud."""
        connection = boto.connect_vpc(
            aws_access_key_id=self._key_id,
            aws_secret_access_key=self._access_key)

        if self._recreate:
            vpc = self._recreate_vpc(connection)
        else:
            vpc = connection.create_vpc(self._network)
            vpc.add_tag('Name', self._cluster_name)

        self._create_security_group(connection, vpc)
        self._create_network(connection, vpc)
        self._setup_placement_group(connection)
