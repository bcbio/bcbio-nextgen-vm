"""
Helper class for collecting and processing information regarding
resources usage.
"""
import collections
import os
import re

from bcbio.graph import graph
import boto.ec2
import boto.iam
import boto.vpc
import pandas
import paramiko
import toolz

from bcbiovm import log as logging
from bcbiovm.common import cluster as cluster_ops
from bcbiovm.common import constant
from bcbiovm.common import utils
from bcbiovm.common import objects
from bcbiovm.provider.aws import icel

LOG = logging.get_logger(__name__)


class Collector(object):

    """
    Collect from the each instances the files which contains
    information regarding resources consumption.

    ::
        # The instance can be used as a function.
        collector = Collector(config, cluster, rawdir)
        collector()

        # Or the `meth: run` can be called
        collector.run()
    """

    COLLECTL_PATH = '/var/log/collectl/*.raw.gz'
    NATDevice = 'NATDevice'

    def __init__(self, config, cluster, rawdir):
        """
        :param config:    elasticluster config file
        :param cluster:   cluster name
        :param rawdir:    directory where to copy raw collectl data files.
        """
        self._output = rawdir
        self._elasticluster = cluster_ops.ElastiCluster(
            provider=constant.PROVIDER.AWS)
        self._elasticluster.load_config(config)
        self._cluster = self._elasticluster.get_cluster(cluster)
        self._aws_config = self._elasticluster.get_config(cluster)
        self._icel = icel.ICELOps(cluster, config)

        self._private_keys = set()
        self._nodes = []

    def __call__(self):
        """Allows an instance of a class to be called as a function."""
        return self.run()

    def _get_ssh_client(self, host, user, port=22, bastion_host=None):
        """Setup and return an instance of :class bcbiovm.utils.SSHClient:."""
        policy = (paramiko.client.AutoAddPolicy() if bastion_host
                  else paramiko.client.RejectPolicy())

        ssh_client = utils.SSHClient(host=host, user=user, port=port)
        ssh_client.client.set_missing_host_key_policy(policy)
        ssh_client.client.load_host_keys(self._cluster.known_hosts_file)
        ssh_client.connect(bastion_host=bastion_host)
        return ssh_client

    def _collectl_files(self, ssh_client):
        """Wrapper over `ssh_client.stat`.

        Process information from `stat` output.

        :param ssh_client:  instance of :class bcbiovm.utils.SSHClient:
        :return:            :class collections.namedtuple: with the
                            following fields: atime, mtime, size and path
        """
        stats = collections.namedtuple("FileInfo", ["atime", "mtine", "size",
                                                    "path"])

        for file_info in ssh_client.stat(path=self.COLLECTL_PATH,
                                         format=("%s", "%X", "%Y", "%n")):
            access_time = int(file_info[0])
            modified_time = int(file_info[1])
            size = int(file_info[2])

            yield stats(access_time, modified_time, size, file_info[3])

    @staticmethod
    def _is_different(path, remote):
        """Check if exists differences between the local and the remote file.

        :path:      the path of the local file
        :remote:    a namedtuple with the information regarding the remote
                    file
        """
        if not os.path.exists(path):
            return True

        if int(os.path.getmtime(path)) != remote.mtine:
            return True

        if os.path.getsize(path) != remote.size:
            return True

        return False

    def _management_target(self):
        """The MGT stores file system configuration information for use by
        the clients and other Lustre components.
        """
        node = self._cluster.get_all_nodes()[0]
        if not node.preferred_ip:
            return None

        ssh_client = self._get_ssh_client(node.preferred_ip, node.image_user)
        disk_info = ssh_client.disk_space("/scratch", ftype="lustre")
        ssh_client.close()

        return None if not disk_info else disk_info[0].split(':')[0]

    def _collect(self, host, user, bastion_host=None):
        """Collect the information from the received host.

        :param host:          the server to connect to
        :param user:          the username to authenticate as (defaults to
                              the current local username)
        :param bastion_host:  the bastion host to connect to
        """
        ssh_client = self._get_ssh_client(host, user, bastion_host)
        for collectl in self._collectl_files(ssh_client):
            destination = os.path.join(self._output,
                                       os.path.basename(collectl.path))
            if not self._is_different(destination, collectl):
                continue
            ssh_client.download_file(collectl.path, destination,
                                     utime=(collectl.atime, collectl.mtime))
        ssh_client.close()

    def _fetch_collectl_lustre(self):
        """Get information from the lustre file system."""
        management_target = self._management_target()
        stack_name = self._icel.stack_name(management_target)
        if not stack_name:
            # FIXME(alexandrucoman): Raise a custom exception
            return

        icel_hosts = self._icel.instances(stack_name)
        for name, host in icel_hosts.items():
            if name == self.NATDevice:
                continue
            self._collect(host, 'ec2-user',
                          bastion_host=icel_hosts[self.NATDevice])

    def available_nodes(self):
        """The available nodes from the received cluster."""
        if not self._nodes:
            for node in self._cluster.get_all_nodes():
                if node.preferred_ip:
                    self._nodes.append(node)

        return self._nodes

    def private_keys(self):
        """The private keys required to access the nodes from the cluster."""
        if not self._private_keys:
            for cluster_type in self._cluster.nodes:
                for node in self._cluster.nodes[cluster_type]:
                    self._private_keys.add(node.user_key_private)

        return self._private_keys

    def run(self):
        """Collect from the each instances the files which contains
        information regarding resources consumption.
        """
        with utils.SSHAgent(self.private_keys()):
            for node in self.available_nodes():
                self._collect(node.preferred_ip, node.image_user)

            self._fetch_collectl_lustre()


class Parser(object):

    """Parse the files collected by :class Collector:"""

    COLLECTL_SUFFIX = '.raw.gz'

    def __init__(self, bcbio_log, rawdir):
        """
        :param bcbio_log:   the bcbio log path
        :param rawdir:      directory to put raw data files
        """
        self._bcbio_log = bcbio_log
        self._rawdir = rawdir

    def __call__(self):
        """Allows an instance of a class to be called as a function."""
        return self.run()

    def _time_frame(self):
        """The bcbio running time frame.

        :return:    an instance of :class collections.namedtuple:
                    with the following fields: start and end
        """
        output = collections.namedtuple("Time", ["start", "end", "steps"])
        bcbio_timings = graph.get_bcbio_timings(self._bcbio_log)
        steps = bcbio_timings.keys()
        return output(min(steps), max(steps), steps)

    def run(self):
        """Parse the information.

        :return: a tuple with three dictionaries, the first one contains
                 an instance of :pandas.DataFrame: for each host, the
                 second one contains information regarding the hardware
                 configuration and the last one contains information
                 regarding timing.
        :type return: tuple
        """
        data_frames = {}
        hardware_info = {}
        time_frame = self._time_frame()

        for collectl_file in sorted(os.listdir(self._rawdir)):
            if not collectl_file.endswith(self.COLLECTL_SUFFIX):
                continue

            collectl_path = os.path.join(self._rawdir, collectl_file)
            data, hardware = graph.load_collectl(
                collectl_path, time_frame.start, time_frame.end)

            if len(data) == 0:
                continue

            host = re.sub(r'-\d{8}-\d{6}\.raw\.gz$', '', collectl_file)
            hardware_info[host] = hardware
            if host not in data_frames:
                data_frames[host] = data
            else:
                data_frames[host] = pandas.concat([data_frames[host], data])

        return (data_frames, hardware_info, time_frame.steps)


class Report(object):

    """
    Collect information from the cluster and create a container
    with them.
    """

    def __init__(self, config, cluster):
        """
        :param config:    elasticluster config file
        :param cluster:   cluster name
        """
        self._information = objects.Report()
        self._elasticluster = cluster_ops.ElastiCluster(
            provider=constant.PROVIDER.AWS)
        self._elasticluster.load_config(config)
        self._cluster_config = self._elasticluster.get_config(cluster)

    def add_cluster_info(self):
        """Add information regarding the cluster."""
        frontend_c = toolz.get_in(["nodes", "frontend"], self._cluster_config)
        compute_c = toolz.get_in(["nodes", "compute"], self._cluster_config)

        cluster = self._information.add_section(
            name="cluster", title="Cluster configuration",
            description="Provide high level details about the setup of the "
                        "current cluster.",
            fields=[{"name": "name"}, {"name": "value"}])
        cluster.add_item([
            "Frontend node",
            {"flavor": frontend_c["flavor"],
             "NFS storage": frontend_c["encrypted_volume_size"]}
        ])
        cluster.add_item([
            "Compute nodes",
            {"count": compute_c["compute_nodes"],
             "flavor": compute_c["flavor"]}
        ])

    def add_iam_info(self):
        """Add information regarding AWS Identity and Access Management."""
        expect_iam_username = "bcbio"
        iam = self._information.add_section(
            name="iam", title="AWS Identity and Access Management")
        iam.add_field("iam", "IAM Users")

        iam_connection = boto.iam.connection.IAMConnection()
        all_users = iam_connection.get_all_users()
        users = toolz.get_in([u"list_users_response", u"list_users_result",
                              "users"], all_users, None)
        if not users:
            LOG.warning("No Identity and Access Management(IAM) users exists.")
            return

        if expect_iam_username in users:
            LOG.info("Expected IAM user %(user)s exists",
                     {"user": expect_iam_username})
        else:
            LOG.warning("IAM user %(user)s does not exist.",
                        {"user": expect_iam_username})

        iam.add_items(users)

    def add_security_groups_info(self):
        """Add information regarding security groups."""
        sg_section = self._information.add_section(
            name="sg", title="Security groups")
        sg_section.add_field("sg", "Security Group")

        region = toolz.get_in(["cloud", "ec2_region"], self._cluster_config)
        expected_sg_name = toolz.get_in(["cluster", "security_group"],
                                        self._cluster_config)

        conn = boto.ec2.connect_to_region(region)
        security_groups = conn.get_all_security_groups()

        if not security_groups:
            LOG.warning("No security groups defined.")
            return

        if expected_sg_name in security_groups:
            LOG.info("Expected security group %(sg_name)s exists.",
                     {"sg_name": expected_sg_name})
        else:
            LOG.warning("Security group %(sg_name)s does not exist.",
                        {"sg_name": expected_sg_name})

        sg_section.add_items(security_groups)

    def add_vpc_info(self):
        """Add information regarding Amazon Virtual Private Cloud."""
        vpc_section = self._information.add_section(
            name="vpc", title="Amazon Virtual Private Cloud.")
        vpc_section.add_field("vpc", "Virtual Private Cloud")

        expected_vpc_name = toolz.get_in(["cloud", "vpc"],
                                         self._cluster_config)
        vpc_connection = boto.vpc.VPCConnection()
        all_vpcs = vpc_connection.get_all_vpcs()
        if not all_vpcs:
            LOG.warning("No VPCs exists.")
            return

        vpc_names = [vpc.tags.get('Name', "") for vpc in all_vpcs]
        if expected_vpc_name in vpc_names:
            LOG.info("VPC %(vpc_name)s exists.",
                     {"vpc_name": expected_vpc_name})
        else:
            LOG.warning("VPC %(vpc_name)s does not exist.",
                        {"vpc_name": expected_vpc_name})
        vpc_section.add_items(vpc_names)

    def add_instance_info(self):
        """Add information regarding each instance from cluster."""
        instance_section = self._information.add_section(
            name="instance", title="Instances from current cluster"
        )
        instance_section.add_field("name", "Name")
        instance_section.add_field("type", "Type")
        instance_section.add_field("state", "State")
        instance_section.add_field("ip", "IP Address")
        instance_section.add_field("placement", "Placement")

        vpcs_by_id = {}
        region = toolz.get_in(["cloud", "ec2_region"], self._cluster_config)
        vpc_name = toolz.get_in(["cloud", "vpc"], self._cluster_config)

        vpc_connection = boto.vpc.VPCConnection()
        ec2_connection = boto.ec2.connect_to_region(region)

        all_vpcs = vpc_connection.get_all_vpcs()
        reservations = ec2_connection.get_all_reservations()

        for vpc in all_vpcs:
            vpcs_by_id[vpc.id] = vpc.tags.get('Name', "")

        for res in reservations:
            for instance in res.instances:
                if vpcs_by_id.get(instance.vpc_id) != vpc_name:
                    continue

                ip_address = instance.ip_address
                if not instance.ip_address:
                    ip_address = instance.private_ip_address

                instance_section.add_item([
                    instance.tags.get("Name", None),
                    instance.instance_type,
                    instance.state, ip_address, instance.placement
                ])

    def digest(self):
        """Return the report."""
        return self._information
