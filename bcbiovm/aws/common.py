"""Common variables used across bcbiovm.aws
"""

import os
import sys

# ansible.utils must be imported before ansible.callbacks.
import ansible.utils
import ansible.callbacks
import ansible.playbook
from elasticluster.conf import Configurator
import elasticluster.main


DEFAULT_EC_CONFIG = os.path.expanduser(
    os.path.join("~", ".bcbio", "elasticluster", "config"))
ANSIBLE_BASE = os.path.join(sys.prefix, "share", "bcbio-vm", "ansible")


class SilentPlaybook(ansible.callbacks.PlaybookCallbacks):
    """Suppress Ansible output when running playbooks."""
    def on_no_hosts_matched(self):
        pass

    def on_no_hosts_remaining(self):
        pass

    def on_task_start(self, name, is_conditional):
        pass

    def on_setup(self):
        pass

    def on_import_for_host(self, host, imported_file):
        pass

    def on_not_import_for_host(self, host, missing_file):
        pass

    def on_play_start(self, pattern):
        pass

    def on_stats(self, stats):
        pass


def add_default_ec_args(parser):
    parser.add_argument("--econfig", default=DEFAULT_EC_CONFIG,
                        help="Elasticluster bcbio configuration file")
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")
    return parser

def bcbio_args_to_ec(ec_args, args):
    """Convert standard bcbio args into elasticluster inputs.
    """
    if args.verbose:
        ec_args.append("-v")
    if args.econfig:
        ec_args = [ec_args[0]] + ["--config", args.econfig] + ec_args[1:]
    return ec_args

def wrap_elasticluster(args):
    """Wrap elasticluster commands to avoid need to call separately.

    - Uses .bcbio/elasticluster as default configuration location.
    - Sets NFS client parameters for elasticluster Ansible playbook. Uses async
      clients which provide better throughput on reads/writes:
      http://nfs.sourceforge.net/nfs-howto/ar01s05.html (section 5.9 for tradeoffs)
    """
    if "-s" not in args and "--storage" not in args:
        # clean up old storage directory if starting a new cluster
        # old pickle files will cause consistent errors when restarting
        storage_dir = os.path.join(os.path.dirname(DEFAULT_EC_CONFIG), "storage")
        std_args = [x for x in args if not x.startswith("-")]
        if len(std_args) >= 3 and std_args[1] == "start":
            cluster = std_args[2]
            pickle_file = os.path.join(storage_dir, "%s.pickle" % cluster)
            if os.path.exists(pickle_file):
                os.remove(pickle_file)
        args = [args[0], "--storage", storage_dir] + args[1:]
    if "-c" not in args and "--config" not in args:
        args = [args[0]] + ["--config", DEFAULT_EC_CONFIG] + args[1:]
    os.environ["nfsoptions"] = "rw,async,nfsvers=3"  # NFS tuning
    sys.argv = args
    try:
        return elasticluster.main.main()
    except SystemExit as exc:
        return exc.args[0]


def ecluster_config(econfig_file, name=None):
    """Load the Elasticluster configuration."""
    storage_dir = os.path.join(os.path.dirname(econfig_file), "storage")
    config = Configurator.fromConfig(econfig_file, storage_dir)
    if not name:
        return config
    if name not in config.cluster_conf:
        raise Exception('Cluster {} is not defined in {}.\n'.format(
            name, os.path.expanduser(econfig_file)))
    return config.cluster_conf[name]


def run_ansible_pb(inventory_path, playbook_path, args, calc_extra_vars=None,
                   ansible_cfg=None):
    """Generalized functionality for running an ansible playbook on
    elasticluster.

    calc_extra_vars is an option function that should return extra variables
    to pass to ansible given the arguments and cluster configuration.
    """
    stats = ansible.callbacks.AggregateStats()
    callbacks = SilentPlaybook()
    runner_cb = ansible.callbacks.DefaultRunnerCallbacks()
    if args.verbose:
        callbacks = ansible.callbacks.PlaybookCallbacks()
        runner_cb = ansible.callbacks.PlaybookRunnerCallbacks(stats)
        ansible.utils.VERBOSITY = args.verbose - 1

    if hasattr(args, "cluster") and hasattr(args, "econfig"):
        cluster_config = ecluster_config(args.econfig, args.cluster)
    else:
        cluster_config = {}
    extra_vars = calc_extra_vars(args, cluster_config) if calc_extra_vars else {}

    if ansible_cfg:
        old_ansible_cfg = os.environ.get('ANSIBLE_CONFIG')
        os.environ['ANSIBLE_CONFIG'] = ansible_cfg
        reload(ansible.constants)

    pb = ansible.playbook.PlayBook(
        playbook=playbook_path,
        extra_vars=extra_vars,
        host_list=inventory_path,
        private_key_file=cluster_config['login']['user_key_private'] if cluster_config else None,
        callbacks=callbacks,
        runner_callbacks=runner_cb,
        forks=10,
        stats=stats)
    status = pb.run()

    if ansible_cfg:
        if old_ansible_cfg:
            os.environ['ANSIBLE_CONFIG'] = old_ansible_cfg
        else:
            del os.environ['ANSIBLE_CONFIG']
        reload(ansible.constants)

    unreachable = []
    failures = {}
    for host, hoststatus in status.items():
        if hoststatus['unreachable']:
            unreachable.append(host)
        if hoststatus['failures']:
            failures[host] = hoststatus['failures']

    if unreachable:
        sys.stderr.write(
            'Unreachable hosts: {}\n'.format(', '.join(unreachable)))
    if failures:
        sys.stderr.write(
            'Failures: {}\n'.format(', '.join([
                '{} ({} failures)'.format(host, num)
                for host, num
                 in failures.items()])))
    if unreachable or failures:
        sys.exit(1)
