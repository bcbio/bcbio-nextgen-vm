"""Manage a cluster's life cycle."""
from __future__ import print_function

import argparse
import os
import sys

from bcbiovm.aws import bootstrap, common

def setup_cmd(awsparser):
    parser_sub_b = awsparser.add_parser("cluster", help="Manage AWS clusters")
    parser_b = parser_sub_b.add_subparsers(title="[cluster specific actions]")

    parser = parser_b.add_parser("bootstrap",
                                 help="Update a bcbio AWS system with "
                                      "the latest code and tools",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser = common.add_default_ec_args(parser)
    parser.add_argument("-R", "--no-reboot",
                        default=False, action="store_true",
                        help="Don't upgrade the cluster host OS and reboot")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Emit verbose output when running "
                             "Ansible playbooks")
    parser.set_defaults(func=bootstrap_cluster)

    parser = parser_b.add_parser("command",
                                 help="Run a script on the bcbio frontend "
                                      "node inside a screen session",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser = common.add_default_ec_args(parser)
    parser.add_argument("script", metavar="SCRIPT",
                        help="Local path of the script to run. The screen "
                             "session name is the basename of the script.")
    parser.set_defaults(func=run_remote)

    parser = parser_b.add_parser("ssh", help="SSH to a bcbio cluster",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser = common.add_default_ec_args(parser)
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Emit verbose output when running "
                             "Ansible playbooks")
    parser.add_argument("args", metavar="ARG", nargs="*",
                        help="Execute the following command on the remote "
                             "machine instead of opening an interactive shell.")
    parser.set_defaults(func=ssh)

    parser = parser_b.add_parser("start", help="Start a bcbio cluster",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser = common.add_default_ec_args(parser)
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Emit verbose output when running "
                             "Ansible playbooks")
    parser.set_defaults(func=start)

    parser = parser_b.add_parser("stop", help="Stop a bcbio cluster",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser = common.add_default_ec_args(parser)
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Emit verbose output when running "
                             "Ansible playbooks")
    parser.set_defaults(func=stop)


def bootstrap_cluster(args):
    """Bootstrap bcbio, or upgrade bcbio on an existing cluster."""
    bootstrap.bootstrap(args)


# ## Run a remote command

def run_remote(args):
    """Run a script on the frontend node inside a screen session."""
    config = common.ecluster_config(args.econfig)
    cluster = config.load_cluster(args.cluster)

    frontend = cluster.get_frontend_node()
    client = frontend.connect(keyfile=cluster.known_hosts_file)

    cmd = "echo $HOME"
    stdin, stdout, stderr = client.exec_command(cmd)
    remote_home = stdout.read().strip()
    remote_file = os.path.join(remote_home, os.path.basename(args.script))
    log_file = "%s.log" % os.path.splitext(remote_file)[0]
    screen_name = os.path.splitext(os.path.basename(remote_file))[0]

    sftp = client.open_sftp()
    sftp.put(args.script, remote_file)
    sftp.close()

    cmd = "chmod a+x %s" % remote_file
    client.exec_command(cmd)
    cmd = "screen -d -m -S {} bash -c '{} &> {}'".format(
        screen_name, remote_file, log_file)
    stdin, stdout, stderr = client.exec_command(cmd)
    stdout.read()
    client.close()
    print("Running {} on AWS in screen session {}".format(
        remote_file, screen_name))


def ssh(args):
    """SSH to a cluster."""
    ec_args = ["elasticluster", "ssh", args.cluster]
    if args.verbose:
        ec_args.append("-{}".format(args.verbose * "v"))
    ec_args.extend(args.args)
    sys.exit(common.wrap_elasticluster(ec_args))


def start(args):
    """Start and bootstrap a cluster."""
    ec_args = ["elasticluster", "start", args.cluster]
    if args.verbose:
        ec_args.append("-{}".format(args.verbose * "v"))
    status = common.wrap_elasticluster(ec_args)
    if status != 0:
        sys.exit(status)
    bootstrap_cluster(args)


def stop(args):
    """Stop a cluster."""
    ec_args = ["elasticluster", "stop", args.cluster]
    if args.verbose:
        ec_args.append("-{}".format(args.verbose * "v"))
    sys.exit(common.wrap_elasticluster(ec_args))
