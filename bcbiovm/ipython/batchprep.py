"""Prepare batch scripts for submitting bcbio_vm jobs.

Automates the process of preparing submission batch scripts, using
the same arguments as standard IPython runs.
"""
import os

from bcbiovm import log as logging
from bcbiovm.common import exception

LOG = logging.get_logger(__name__)


# names that indicate we're running on a dedicated AWS queue
AWS_QUEUES = set(["cloud"])


def _get_ipython_cmdline(args):
    """Translate arguments back into a standard bcbio_vm ipython
    submission command.
    """
    command = ["bcbio_vm.py", "ipython", args.sample_config,
               args.scheduler, args.queue,
               "--numcores", str(args.numcores)]
    has_timelimit = False
    for resource in args.resources:
        command.extend(["-r", resource])
        if resource.startswith("timelimit"):
            has_timelimit = True

    if not has_timelimit and args.queue in AWS_QUEUES:
        command.extend(["-r", "timelimit=0"])

    for name in ("timeout", "retries", "tag", "tmpdir", "fcdir",
                 "systemconfig"):
        argument = getattr(args, name)
        if argument:
            command.extend(["--%s" % name, str(argument)])

    return " ".join(command)


def submit_script(args):
    """Automates the process of preparing submission batch scripts,
    using the same arguments as standard IPython runs.
    """
    out_file = os.path.join(os.getcwd(), "bcbio_submit.sh")
    LOG.info("Writing submission script for %s to %s",
             args.scheduler, out_file)

    with open(out_file, "w") as out_handle:
        out_handle.write("#!/bin/bash\n")
        out_handle.write(_get_scheduler_arguments(args) + "\n")
        out_handle.write(_get_ipython_cmdline(args) + "\n")

    return out_file


def _get_scheduler_arguments(args):
    """Scheduler arguments factory."""
    commands = {"slurm": _get_slurm_cmds, "sge": _get_sge_cmds,
                "lsf": _get_lsf_cmds, "torque": _get_torque_cmds,
                "pbspro": _get_torque_cmds}
    try:
        return commands[args.scheduler](args)
    except KeyError:
        raise exception.NotSupported("Batch script preparation for %s "
                                     "not yet supported" % args.scheduler)


def _get_slurm_cmds(args):
    """Required arguments for the SLURM scheduler."""
    cmds = ["--cpus-per-task=1", "--mem=2000", "-p %s" % args.queue]
    timelimit = "0" if args.queue in AWS_QUEUES else "1-00:00:00"
    for r in args.resources:
        if r.startswith("timelimit"):
            _, timelimit = r.split("=")
    cmds += ["-t %s" % timelimit]
    if args.tag:
        cmds += ["-J %s-submit" % args.tag]
    return "\n".join("#SBATCH %s" % x for x in cmds)


def _get_sge_cmds(args):
    """Required arguments for the SGE scheduler."""
    cmds = ["-cwd", "-j y", "-S /bin/bash"]
    if args.queue:
        cmds += ["-q %s" % args.queue]
    if args.tag:
        cmds += ["-N %s-submit" % args.tag]
    return "\n".join("#$ %s" % x for x in cmds)


def _get_lsf_cmds(args):
    """Required arguments for the LSF scheduler."""
    cmds = ["-q %s" % args.queue, "-n 1"]
    if args.tag:
        cmds += ["-J %s-submit" % args.tag]
    return "\n".join("#BSUB %s" % x for x in cmds)


def _get_torque_cmds(args):
    """Required arguments for the SLURM scheduler."""
    cmds = ["-V", "-j oe", "-q %s" % args.queue, "-l nodes=1:ppn=1"]
    if args.tag:
        cmds += ["-N %s-submit" % args.tag]
    return "\n".join("#PBS %s" % x for x in cmds)
