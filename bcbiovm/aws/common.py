"""Common variables used across bcbiovm.aws
"""

import os
import sys

# ansible.utils must be imported before ansible.callbacks.
import ansible.utils
import ansible.callbacks
import ansible.playbook
from elasticluster.conf import Configurator


DEFAULT_EC_CONFIG = os.path.expanduser(
    os.path.join("~", ".bcbio", "elasticluster", "config"))


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


def run_ansible_pb(playbook_path, args, calc_extra_vars=None):
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
        inventory_path = os.path.join(os.path.dirname(args.econfig),
                                      "storage",
                                      "ansible-inventory.%s" % args.cluster)
        cluster_config = ecluster_config(args.cluster, args.econfig)
    else:
        cluster_config = {}
        inventory_path = os.path.join(os.path.dirname(playbook_path), "standard_hosts.txt")
    extra_vars = calc_extra_vars(args, cluster_config) if calc_extra_vars else {}

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
