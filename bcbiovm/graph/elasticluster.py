"""Pull data from an Elasticluster cluster to generate graphs from."""

import contextlib
import os
import re
import subprocess

import elasticluster
import paramiko

from bcbiovm.aws.common import ecluster_config


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


def fetch_collectl(econfig_file, cluster_name, datadir):
    config = ecluster_config(econfig_file)
    cluster = config.load_cluster(cluster_name)

    keys = set()
    for type in cluster.nodes:
        for node in cluster.nodes[type]:
            keys.add(node.user_key_private)

    with ssh_agent(keys):
        keys = []
        for type in cluster.nodes:
            for node in cluster.nodes[type]:
                client = paramiko.client.SSHClient()
                client.set_missing_host_key_policy(
                    paramiko.client.RejectPolicy)
                client.load_host_keys(cluster.known_hosts_file)
                client.connect(
                    node.preferred_ip, username=node.image_user)

                command = 'stat -c "%s %Y %n" /var/log/collectl/*.raw.gz'
                stdin, stdout, stderr = client.exec_command(command)
                raws = stdout.read().strip().split('\n')

                for raw in raws:
                    size, mtime, remote_raw = raw.split()
                    mtime = int(mtime)
                    size = int(size)

                    raw_basename = os.path.basename(remote_raw)
                    local_raw = os.path.join(datadir, raw_basename)
                    if os.path.exists(local_raw):
                        if (int(os.path.getmtime(local_raw)) == mtime and
                            os.path.getsize(local_raw) == size):
                            continue
                        os.unlink(local_raw)

                    command = 'cat {}'.format(remote_raw)
                    stdin, stdout, stderr = client.exec_command(command)
                    with open(local_raw, 'wb') as fp:
                        fp.write(stdout.read())
                    os.utime(local_raw, (mtime, mtime))
