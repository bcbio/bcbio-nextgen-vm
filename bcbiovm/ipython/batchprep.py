"""Prepare batch scripts for submitting bcbio_vm jobs.

Automates the process of preparing submission batch scripts, using the same arguments as
standard IPython runs.
"""
import os

def _get_ipython_cmdline(args):
    """Translate arguments back into a standard bcbio_vm ipython submission command.
    """
    cmd = ["bcbio_vm.py", "ipython", args.sample_config, args.scheduler, args.queue,
           "--numcores", str(args.numcores)]
    for resource in args.resources:
        cmd += ["-r", resource]
    for opt_arg in ["timeout", "retries", "tag", "tmpdir", "fcdir", "systemconfig"]:
        if getattr(args, opt_arg):
            cmd += ["--%s" % opt_arg, str(getattr(args, opt_arg))]
    return " ".join(cmd)

def submit_script(args):
    out_file = os.path.join(os.getcwd(), "bcbio_submit.sh")
    with open(out_file, "w") as out_handle:
        out_handle.write("#!/bin/bash\n")
        out_handle.write(_get_scheduler_cmds(args) + "\n")
        out_handle.write(_get_ipython_cmdline(args) + "\n")
    print("Submission script for %s written to %s" % (args.scheduler, out_file))
    print("Start analysis with: %s %s" % (_get_submit_cmd(args.scheduler), out_file))

def _get_scheduler_cmds(args):
    cmds = {"slurm": _get_slurm_cmds,
            "sge": _get_sge_cmds,
            "lsf": _get_lsf_cmds,
            "torque": _get_torque_cmds,
            "pbspro": _get_torque_cmds}
    try:
        return cmds[args.scheduler](args)
    except KeyError:
        raise NotImplementedError("Batch script preparation for %s not yet supported" % args.scheduler)

def _get_slurm_cmds(args):
    cmds = ["--cpus-per-task=1", "--mem=2000", "-p %s" % args.queue]
    timelimit = "1-00:00:00"
    for r in args.resources:
        if r.startswith("timelimit"):
            _, timelimit = r.split("=")
    cmds += ["-t %s" % timelimit]
    if args.tag:
        cmds += ["-J %s-submit" % args.tag]
    return "\n".join("#SBATCH %s" % x for x in cmds)

def _get_sge_cmds(args):
    cmds = ["-cwd", "-j y", "-S /bin/bash"]
    if args.queue:
        cmds += ["-q %s" % args.queue]
    if args.tag:
        cmds += ["-N %s-submit" % args.tag]
    return "\n".join("#$ %s" % x for x in cmds)

def _get_lsf_cmds(args):
    cmds = ["-q %s" % args.queue, "-n 1"]
    if args.tag:
        cmds += ["-J %s-submit" % args.tag]
    return "\n".join("#BSUB %s" % x for x in cmds)

def _get_torque_cmds(args):
    cmds = ["-V", "-j oe", "-q %s" % args.queue, "-l nodes=1:ppn=1"]
    if args.tag:
        cmds += ["-N %s-submit" % args.tag]
    return "\n".join("#PBS %s" % x for x in cmds)

def _get_submit_cmd(scheduler):
    cmds = {"slurm": "sbatch",
            "sge": "qsub",
            "lsf": "bsub",
            "torque": "qsub",
            "pbspro": "qsub"}
    return cmds[scheduler]
