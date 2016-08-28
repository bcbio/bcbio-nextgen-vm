"""Utilities to help with developing using bcbio inside of docker.
"""
import argparse
import collections
import copy
import datetime
import glob
import os
import shutil
import subprocess

import numpy
import yaml

from bcbio import utils
from bcbio.distributed import objectstore
from bcbio.pipeline import genome
from bcbio.provenance import do

from bcbiovm.aws import common
from bcbiovm.docker import defaults, install, manage, mounts

# default information about docker container
DOCKER = {"port": 8085,
          "biodata_dir": "/usr/local/share/bcbio-nextgen",
          "work_dir": "/mnt/work",
          "image_url": "bcbio/bcbio"}

# Available genomes and indexes
SUPPORTED_GENOMES = ["GRCh37", "hg19", "hg38", "hg38-noalt", "mm10", "mm9",
                     "rn6", "rn5", "canFam3", "dm3", "galGal4", "phix",
                     "pseudomonas_aeruginosa_ucbpp_pa14", "sacCer3", "TAIR10",
                     "WBcel235", "xenTro3", "Zv9", "GRCz10"]
SUPPORTED_INDEXES = ["bowtie", "bowtie2", "bwa", "novoalign", "rtg", "snap",
                     "star", "ucsc", "seq", "hisat2"]

def add_biodata_args(parser):
    """Add standard arguments for preparing biological data to a command line arg parser.
    """
    parser.add_argument("--genomes", help="Genomes to download",
                        action="append", default=[],
                        choices=SUPPORTED_GENOMES)
    parser.add_argument("--aligners", help="Aligner indexes to download",
                        action="append", default=[],
                        choices=SUPPORTED_INDEXES)
    parser.add_argument("--datatarget", help="Data to install. Allows customization or install of extra data.",
                        action="append", default=[],
                        choices=["variation", "rnaseq", "smallrna", "gemini", "cadd", "vep", "dbnsfp",
                                 "battenberg", "kraken"])
    return parser

def setup_cmd(subparsers):
    parser = subparsers.add_parser("devel", help="Utilities to help with develping using bcbion inside of docker")
    psub = parser.add_subparsers(title="[devel commands]")

    iparser = psub.add_parser("setup_install", help="Run a python setup.py install inside of the current directory")
    iparser.add_argument("-i", "--image", help="Image name to write updates to",
                         default=install.DEFAULT_IMAGE)
    iparser.set_defaults(func=_run_setup_install)

    iparser = psub.add_parser("upgrade_tools", help="Upgrade tool installation inside current docker container")
    iparser.add_argument("-i", "--image", help="Image name to write updates to",
                         default=install.DEFAULT_IMAGE)
    iparser.add_argument("--toolplus", help="Specify additional tool categories to install",
                         action="append", default=[], type=_check_toolplus)
    iparser.set_defaults(func=_run_upgrade_tools)

    rparser = psub.add_parser("register", help="Register a file (like GATK jar) with bioconda script")
    rparser.add_argument("-i", "--image", help="Image name to write updates to",
                         default=install.DEFAULT_IMAGE)
    rparser.add_argument("name", help="Program to register", choices=["gatk"])
    rparser.add_argument("file_name", help="File to pass to register command")
    rparser.set_defaults(func=_run_register)

    sparser = psub.add_parser("system", help="Update bcbio system file with a given core and memory/core target")
    sparser.add_argument("cores", help="Target cores to use for multi-core processes")
    sparser.add_argument("memory", help="Target memory per core, in Mb (1000 = 1Gb)")
    sparser.set_defaults(func=_run_system_update)

    dparser = psub.add_parser("biodata", help="Upload pre-prepared biological data to cache")
    dparser.add_argument("--prepped", help="Start with an existing set of cached data to output directory.")
    dparser = add_biodata_args(dparser)
    dparser.set_defaults(func=_run_biodata_upload)

    dbparser = psub.add_parser("dockerbuild", help="Build docker image and export to S3")
    dbparser.add_argument("-b", "--bucket", default="bcbio_nextgen",
                          help="S3 bucket to upload the gzipped docker image to")
    dbparser.add_argument("-t", "--buildtype", default="full", choices=["full", "code"],
                          help=("Type of docker build to do. full is all code and third party tools. "
                                "code is only bcbio-nextgen code."))
    dbparser.add_argument("-d", "--rundir", default="/tmp/bcbio-docker-build",
                          help="Directory to run docker build in")
    parser.add_argument("-q", "--quiet", dest="verbose", action="store_false", default=True,
                        help="Quiet output when running Ansible playbooks")
    dbparser.set_defaults(func=_run_docker_build)

def _check_toolplus(x):
    """Parse options for adding non-standard/commercial tools like GATK and MuTecT.
    """
    Tool = collections.namedtuple("Tool", ["name", "fname"])
    if "=" in x and len(x.split("=")) == 2:
        name, fname = x.split("=")
        fname = os.path.normpath(os.path.realpath(fname))
        if not os.path.exists(fname):
            raise argparse.ArgumentTypeError("Unexpected --toolplus argument for %s. File does not exist: %s"
                                             % (name, fname))
        return Tool(name, fname)
    else:
        raise argparse.ArgumentTypeError("Unexpected --toolplus argument. Expect toolname=filename.")

# ## Install code to docker image

def _run_cmd_commit(cmd, bmounts, args):
    cmd = ["docker", "run", "-i", "-d", "--net=host"] + bmounts + [args.image] + \
          ["bash", "-l", "-c", cmd]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    cid = process.communicate()[0].strip()
    do.run(["docker", "attach", "--no-stdin", cid], "Running in docker container: %s" % cid,
           log_stdout=True)
    subprocess.check_call(["docker", "commit", cid, args.image])
    subprocess.check_call(["docker", "rm", cid], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

def _run_setup_install(args):
    """Install python code from a bcbio-nextgen development tree inside of docker.
    """
    bmounts = ["-v", "%s:%s" % (os.getcwd(), "/tmp/bcbio-nextgen")]
    cmd = ("rm -rf /usr/local/share/bcbio-nextgen/anaconda/lib/python2.7/site-packages/bcbio && "
           "rm -rf /usr/local/share/bcbio-nextgen/anaconda/lib/python2.7/site-packages/bcbio_nextgen-*.egg-info && "
           "cd /tmp/bcbio-nextgen && "
           "/usr/local/share/bcbio-nextgen/anaconda/bin/python setup.py install")
    _run_cmd_commit(cmd, bmounts, args)
    print("Updated bcbio-nextgen install in docker container: %s" % args.image)

def _run_upgrade_tools(args):
    cmd = "bcbio_nextgen.py upgrade --tools"
    mounts = []
    for tool in args.toolplus:
        mounts.extend(["-v", "%s:%s" % (tool.fname, tool.fname)])
        cmd += " --toolplus %s=%s" % (tool.name, tool.fname)
    _run_cmd_commit(cmd, mounts, args)
    print("Updated bcbio-nextgen tools in docker container: %s" % args.image)

def _run_register(args):
    fname = os.path.abspath(args.file_name)
    cmd = "%s-register %s --noversioncheck" % (args.name, fname)
    mounts = ["-v", "%s:%s" % (fname, fname)]
    _run_cmd_commit(cmd, mounts, args)
    print("Registered %s with %s" % (args.name, fname))

# ## Update bcbio_system.yaml

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
    mod_swap = {"G": "M", "g": "m"}
    cur_mem, orig_mod = _get_cur_mem(key, cur)
    if cur_mem >= common_mem * 1.5:
        return cur
    else:
        new_val = "%s%s" % (target, mod_swap.get(orig_mod, orig_mod))
        if key == "jvm_opts":
            out = cur
            out[-1] = "-Xmx%s" % new_val
        else:
            out = new_val
        return out

# ## Build docker images

def _run_docker_build(args):
    playbook = os.path.join(common.ANSIBLE_BASE, "bcbio_vm_docker_local.yml")
    inventory_path = os.path.join(common.ANSIBLE_BASE, "standard_hosts.txt")
    def _setup_args(args, cluster_config):
        return {"bcbio_bucket": args.bucket, "docker_buildtype": args.buildtype,
                "bcbio_dir": args.rundir}
    common.run_ansible_pb(inventory_path, playbook, args, _setup_args)

# ## Upload pre-build biological data

def _run_biodata_upload(args):
    """Manage preparation of biodata on a local machine, uploading to S3 in pieces.
    """
    args = defaults.update_check_args(args, "biodata not uploaded")
    args = install.docker_image_arg(args)
    for gbuild in args.genomes:
        print("Preparing %s" % gbuild)
        if args.prepped:
            for target in ["samtools"] + args.aligners:
                genome.download_prepped_genome(gbuild, {}, target, False, args.prepped)
            print("Downloaded prepped %s to %s. Edit and re-run without --prepped to upload"
                  % (gbuild, args.prepped))
            return
        cl = ["upgrade", "--genomes", gbuild]
        for a in args.aligners:
            cl += ["--aligners", a]
        for t in args.datatarget:
            cl += ["--datatarget", t]
        dmounts = mounts.prepare_system(args.datadir, DOCKER["biodata_dir"])
        manage.run_bcbio_cmd(args.image, dmounts, cl)
        print("Uploading %s" % gbuild)
        gdir = _get_basedir(args.datadir, gbuild)
        basedir, genomedir = os.path.split(gdir)
        assert genomedir == gbuild
        with utils.chdir(basedir):
            all_dirs = sorted(os.listdir(gbuild))
            _upload_biodata(gbuild, "seq", all_dirs)
            for aligner in args.aligners + ["rtg"]:
                _upload_biodata(gbuild, genome.REMAP_NAMES.get(aligner, aligner), all_dirs)

def _upload_biodata(gbuild, target, all_dirs):
    """Upload biodata for a specific genome build and target to S3.
    """
    if target == "seq":
        want_dirs = set(["coverage", "editing", "prioritization", "rnaseq",
                         "seq", "snpeff", "srnaseq", "validation",
                         "variation", "vep"])
        target_dirs = [x for x in all_dirs if x in want_dirs]
    else:
        target_dirs = [x for x in all_dirs if x == target]
    target_dirs = [os.path.join(gbuild, x) for x in target_dirs]
    fname = objectstore.BIODATA_INFO["s3"].format(build=gbuild, target=target)
    remotef = objectstore.parse_remote(fname)
    conn = objectstore.connect(fname)
    bucket = conn.get_bucket(remotef.bucket)
    key = bucket.get_key(remotef.key)
    if not key:
        keyname = remotef.key
        bucketname = remotef.bucket
        target_dirs = " ".join(target_dirs)
        cmd = ("tar -cvpf - {target_dirs} | pigz -c | "
               "gof3r put --no-md5 -k {keyname} -b {bucketname} "
               "-m x-amz-storage-class:REDUCED_REDUNDANCY -m x-amz-acl:public-read")
        do.run(cmd.format(**locals()), "Upload pre-prepared genome data: %s %s" % (gbuild, target))

def _get_basedir(datadir, target_genome):
    """Retrieve base directory for uploading.
    """
    genome_dir = os.path.join(datadir, "genomes")
    for dirname in glob.glob(os.path.join(genome_dir, "*", "*")):
        if dirname.endswith("/%s" % target_genome):
            return dirname
