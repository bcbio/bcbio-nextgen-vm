"""Pull data from an Elasticluster cluster to generate graphs from."""
from __future__ import print_function

import contextlib
import os
import re
import subprocess

import elasticluster
import paramiko

from bcbiovm.aws.common import ecluster_config
from bcbiovm.aws import icel
from bcbio.graph import graph as bcbio_graph


@contextlib.contextmanager
def ssh_agent(private_key_paths=[]):
    output = subprocess.check_output(['ssh-agent', '-s'])
    for line in output.split('\n'):
        matches = re.search(r'^([A-Z0-9_]+)=(.+?);.*', line)
        if matches:
            os.environ[matches.group(1)] = matches.group(2)

    with open('/dev/null', 'w') as dev_null:
        for key_path in private_key_paths:
            subprocess.check_call(
                ['ssh-add', key_path], stdout=dev_null, stderr=dev_null)

    yield

    with open('/dev/null', 'w') as dev_null:
        subprocess.call(['ssh-agent', '-k'], stdout=dev_null)

def _pull_collectl_data(host, username, datadir, bcbio_log, ssh_client,
                        bastion_host=None, verbose=False):
    if verbose:
        print('Connecting to {}{}...'.format(
            host, ' via {}'.format(bastion_host) if bastion_host else ''))

    proxy_command = None
    if bastion_host:
        proxy_command = paramiko.ProxyCommand(
            'ssh -o VisualHostKey=no -W {}:22 ec2-user@{}'.format(
                host, bastion_host))
    ssh_client.connect(host, username=username, allow_agent=True,
        sock=proxy_command)

    command = 'stat -c "%s %Y %n" /var/log/collectl/*.raw.gz'
    if verbose:
        print('Running "{}"...'.format(command))
    stdin, stdout, stderr = ssh_client.exec_command(command)
    raws = stdout.read().strip()
    if not raws:
        return

    # Only load filenames withing sampling timerage
    time_frame = bcbio_graph.log_time_frame(bcbio_log)

    for raw in raws.split('\n'):
        if bcbio_graph.rawfile_within_timeframe(raw, time_frame):
            size, mtime, remote_raw = raw.split()
            mtime = int(mtime)
            size = int(size)

            raw_basename = os.path.basename(remote_raw)
            local_raw = os.path.join(datadir, raw_basename)
            if (os.path.exists(local_raw) and
                int(os.path.getmtime(local_raw)) == mtime and
                os.path.getsize(local_raw) == size):
                # Remote file hasn't changed, don't re-fetch it.
                continue

            command = 'cat {}'.format(remote_raw)
            # Only transfer the remote raw locally if it falls within our
            # sampling timeframe.

            if verbose:
                print('Running "{}" on {}...'.format(command, host))
            stdin, stdout, stderr = ssh_client.exec_command(command)
            with open(local_raw, 'wb') as fp:
                fp.write(stdout.read())
            os.utime(local_raw, (mtime, mtime))

    ssh_client.close()


def _mgt_addr_for_scratch_on(cluster, ssh):
    node = cluster.get_all_nodes()[0]
    if not node.preferred_ip:
        return None

    ssh.connect(node.preferred_ip, username=node.image_user, allow_agent=True)
    stdin, stdout, stderr = ssh.exec_command('df -t lustre /scratch')

    df_scratch = stdout.read().strip()
    if not df_scratch:
        return None

    return df_scratch.split('\n')[1].split()[0].split(':')[0]


def _fetch_collectl_lustre(cluster, ssh, datadir, aws_config, verbose):
    mgt_addr = _mgt_addr_for_scratch_on(cluster, ssh)
    if not mgt_addr:
        return

    stack_name = icel.get_stack_name(mgt_addr, aws_config)
    if not stack_name:
        raise Exception('Unable to determine stack name '
            'for ICEL MGT %s'.format(mgt_addr))

    icel_hosts = icel.get_instances(stack_name, aws_config)
    for name, addr in icel_hosts.items():
        if name == 'NATDevice':
            continue
        _pull_collectl_data(
            addr, 'ec2-user', datadir, bcbio_log, ssh,
            bastion_host=icel_hosts['NATDevice'], verbose=verbose)


def fetch_collectl(econfig_file, cluster_name, bcbio_log, datadir, verbose=False):
    # local cluster, bypassing elasticluster
    if "local" in cluster_name:
	import getpass
        with ssh_agent():
            ssh = paramiko.client.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())

	    for host in bcbio_graph.get_bcbio_nodes(bcbio_log):
		_pull_collectl_data(host, getpass.getuser(), datadir, 
                                    bcbio_log, ssh, verbose=verbose)

    # elasticluster
    else:
	config = ecluster_config(econfig_file)
	cluster = config.load_cluster(cluster_name)

	keys = set()
	for type in cluster.nodes:
	    for node in cluster.nodes[type]:
		keys.add(node.user_key_private)

	with ssh_agent(keys):
	    ssh = paramiko.client.SSHClient()
	    ssh.set_missing_host_key_policy(paramiko.client.RejectPolicy())
	    ssh.load_host_keys(cluster.known_hosts_file)

	    for node in cluster.get_all_nodes():
		if not node.preferred_ip:
		    # Instance is unavailable.
		    continue
		_pull_collectl_data(
		    node.preferred_ip, node.image_user, datadir, 
                    bcbio_log, ssh, verbose=verbose)

	    # FIXME: load SSH host keys from ICEL instances.
	    ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
	    aws_config = config.cluster_conf[cluster_name]['cloud']
	    _fetch_collectl_lustre(cluster, ssh, datadir, aws_config, verbose)
