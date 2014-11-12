"""Utilities to help with developing using bcbio inside of docker.
"""
import copy
import datetime
import math
import os
import shutil
import subprocess

import numpy
import yaml

from bcbio.provenance import do
from bcbiovm.docker import defaults, install

def setup_cmd(subparsers):
    parser = subparsers.add_parser("devel", help="Utilities to help with develping using bcbion inside of docker")
    psub = parser.add_subparsers(title="[devel commands]")

    iparser = psub.add_parser("setup_install", help="Run a python setup.py install inside of the current directory")
    iparser.add_argument("-i", "--image", help="Image name to write updates to",
                         default=install.DEFAULT_IMAGE)
    iparser.set_defaults(func=_run_setup_install)

    sparser = psub.add_parser("system", help="Update bcbio system file with a given core and memory/core target")
    sparser.add_argument("cores", help="Target cores to use for multi-core processes")
    sparser.add_argument("memory", help="Target memory per core, in Mb (1000 = 1Gb)")
    sparser.set_defaults(func=_run_system_update)

def _run_setup_install(args):
    """Install python code from a bcbio-nextgen development tree inside of docker.
    """
    mounts = ["-v", "%s:%s" % (os.getcwd(), "/tmp/bcbio-nextgen")]
    cmd = ["docker", "run", "-i", "-d"] + mounts + [args.image] + \
          ["bash", "-l", "-c",
           ("rm -rf /usr/local/share/bcbio-nextgen/anaconda/lib/python2.7/site-packages/bcbio && "
            "cd /tmp/bcbio-nextgen && "
            "/usr/local/share/bcbio-nextgen/anaconda/bin/python setup.py install")]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    cid = process.communicate()[0].strip()
    do.run(["docker", "attach", "--no-stdin", cid], "Running in docker container: %s" % cid,
           log_stdout=True)
    subprocess.check_call(["docker", "commit", cid, args.image])
    subprocess.check_call(["docker", "rm", cid], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print("Updated bcbio-nextgen install in docker container: %s" % args.image)

def _run_system_update(args):
    """Update bcbio_system.yaml file with a given target of cores and memory.
    """
    mem_types = set(["memory", "jvm_opts"])
    args = defaults.update_check_args(args, "Could not do upgrade of bcbio_system.yaml")
    system_file = os.path.join(args.datadir, "galaxy", "bcbio_system.yaml")
    with open(system_file) as in_handle:
        config = yaml.safe_load(in_handle)
    out = copy.deepcopy(config)
    mems = []
    for attrs in config.get("resources", {}).itervalues():
        for key, value in attrs.iteritems():
            if key in mem_types:
                mems.append((key, value))
    common_mem = _calculate_common_memory(mems)
    for prog, attrs in config.get("resources", {}).iteritems():
        for key, value in attrs.iteritems():
            if key == "cores":
                out['resources'][prog][key] = int(args.cores)
            elif key in mem_types:
                out["resources"][prog][key] = _update_memory(key, value, args.memory,
                                                             common_mem)
    bak_file = system_file + ".bak%s" % datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    shutil.move(system_file, bak_file)
    with open(system_file, "w") as out_handle:
        yaml.safe_dump(out, out_handle, default_flow_style=False, allow_unicode=False)

def _get_cur_mem(key, val):
    if key == "memory":
        cur_mem = val
    elif key == "jvm_opts":
        cur_mem = val[1].replace("-Xmx", "")
    cur_val = int(cur_mem[:-1])
    cur_mod = cur_mem[-1:]
    if cur_mod.lower() == "g":
        cur_val = cur_val * 1000
    else:
        assert cur_mod.lower() == "m"
    return cur_val, cur_mod

def _calculate_common_memory(kvs):
    """Get the median memory specification, in megabytes.
    """
    mems = []
    for key, val in kvs:
        cur_val, _ = _get_cur_mem(key, val)
        mems.append(cur_val)
    return numpy.median(mems)

def _update_memory(key, cur, target, common_mem):
    """Update memory specifications to match target.

    Handles JVM options and both megabyte and gigabyte specifications.
    `target` is in megabytes. Does not adjust down memory that is more
    than 1.5x the current common memory setting, assuming these are pre-set for
    higher memory requirements.
    """
    cur_mem, orig_mod = _get_cur_mem(key, cur)
    if cur_mem >= common_mem * 1.5:
        return cur
    else:
        if orig_mod.lower() == "g":
            target = int(math.floor(float(target) / 1000.0))
        else:
            target = int(target)
        new_val = "%s%s" % (target, orig_mod)
        if key == "jvm_opts":
            out = cur
            out[-1] = "-Xmx%s" % new_val
        else:
            out = new_val
        return out
