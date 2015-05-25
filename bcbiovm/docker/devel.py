"""Utilities to help with developing using bcbio inside of docker.
"""
import copy
import datetime
import glob
import os
import shutil
import subprocess

import boto
import numpy
import yaml

from bcbio import utils
from bcbio.distributed import objectstore
from bcbio.pipeline import genome
from bcbio.provenance import do

from bcbiovm.common import cluster as clusterops
from bcbiovm.common import constant
from bcbiovm.docker import defaults, install, manage, mounts


def add_biodata_args(parser):
    """Add standard arguments for preparing biological data to a
    command line arg parser.
    """
    parser.add_argument(
        "--genomes", help="Genomes to download",
        action="append", default=[],
        choices=["GRCh37", "hg19", "mm10", "mm9", "rn5", "canFam3", "dm3",
                 "Zv9", "phix", "sacCer3", "xenTro3", "TAIR10", "WBcel235"])
    parser.add_argument(
        "--aligners", help="Aligner indexes to download",
        action="append", default=[],
        choices=["bowtie", "bowtie2", "bwa", "novoalign", "star", "ucsc"])
    return parser


def setup_cmd(subparsers):
    parser = subparsers.add_parser(
        "devel",
        help="Utilities to help with develping using bcbion inside of docker")
    psub = parser.add_subparsers(title="[devel commands]")

    sparser = psub.add_parser(
        "system",
        help=("Update bcbio system file with a given core "
              "and memory/core target"))
    sparser.add_argument(
        "cores",
        help="Target cores to use for multi-core processes")
    sparser.add_argument(
        "memory",
        help="Target memory per core, in Mb (1000 = 1Gb)")
    sparser.set_defaults(func=_run_system_update)

    dparser = psub.add_parser(
        "biodata",
        help="Upload pre-prepared biological data to cache")
    dparser.add_argument(
        "--prepped",
        help="Start with an existing set of cached data to output directory.")
    dparser = add_biodata_args(dparser)
    dparser.set_defaults(func=_run_biodata_upload)

    dbparser = psub.add_parser(
        "dockerbuild",
        help="Build docker image and export to S3")
    dbparser.add_argument(
        "-b", "--bucket", default="bcbio_nextgen",
        help="S3 bucket to upload the gzipped docker image to")
    dbparser.add_argument(
        "-t", "--buildtype", default="full", choices=["full", "code"],
        help=("Type of docker build to do. full is all code and third party"
              " tools. code is only bcbio-nextgen code."))
    dbparser.add_argument(
        "-d", "--rundir", default="/tmp/bcbio-docker-build",
        help="Directory to run docker build in")
    parser.add_argument(
        "-q", "--quiet", dest="verbose", action="store_false",
        default=True, help="Quiet output when running Ansible playbooks")
    dbparser.set_defaults(func=_run_docker_build)


def run_setup_install(args):
    """Install python code from a bcbio-nextgen development tree
    inside of docker.
    """
    # Install code to docker image
    bmounts = ["-v", "%s:%s" % (os.getcwd(), "/tmp/bcbio-nextgen")]
    bash_cmd = ("rm -rf /usr/local/share/bcbio-nextgen/anaconda/lib/"
                "python2.7/site-packages/bcbio && cd /tmp/bcbio-nextgen && "
                "/usr/local/share/bcbio-nextgen/anaconda/bin/python "
                "setup.py install")
    cmd = ["docker", "run", "-i", "-d", "--net=host"]
    cmd.extend(bmounts)
    cmd.extend([args.image, "bash", "-l", "-c", bash_cmd])
    # TODO(alexandrucoman): Use bcbiovm.common.utils.execute
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    cid = process.communicate()[0].strip()
    do.run(["docker", "attach", "--no-stdin", cid],
           "Running in docker container: %s" % cid,
           log_stdout=True)

    # TODO(alexandrucoman): Use bcbiovm.common.utils.execute
    subprocess.check_call(["docker", "commit", cid, args.image])
    subprocess.check_call(["docker", "rm", cid], stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT)
    print("Updated bcbio-nextgen install in docker container: %s" % args.image)


def _run_system_update(args):
    """Update bcbio_system.yaml file with a given target of cores
    and memory.
    """
    # Update bcbio_system.yaml
    mem_types = set(["memory", "jvm_opts"])
    args = defaults.update_check_args(
        args, "Could not do upgrade of bcbio_system.yaml")
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
                out["resources"][prog][key] = _update_memory(key, value,
                                                             args.memory,
                                                             common_mem)
    # TODO(alexandrucoman): Add an utility for creating backup files
    now = datetime.datetime.now()
    bak_file = system_file + ".bak%s" % now.strftime("%Y-%m-%d-%H-%M-%S")
    shutil.move(system_file, bak_file)
    with open(system_file, "w") as out_handle:
        yaml.safe_dump(out, out_handle, default_flow_style=False,
                       allow_unicode=False)


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


def _run_docker_build(args):
    # Build docker images
    inventory_path = os.path.join(constant.PATH.ANSIBLE_BASE,
                                  "standard_hosts.txt")

    def extra_vars(cluster_config):
        """Extra variables to inject into a playbook."""
        return {
            "bcbio_bucket": args.bucket,
            "docker_buildtype": args.buildtype,
            "bcbio_dir": args.rundir
        }

    playbook = clusterops.AnsiblePlaybook(
        inventory_path=inventory_path,
        playbook_path=constant.PLAYBOOK.DOCKER_LOCAl,
        config=args.econfig,
        cluster=args.cluster,
        verbose=args.verbose,
        extra_vars=extra_vars)
    return playbook.run()


def _run_biodata_upload(args):
    """Manage preparation of biodata on a local machine, uploading
    to S3 in pieces.
    """
    # ## Upload pre-build biological data
    args = defaults.update_check_args(args, "biodata not uploaded")
    args = install.docker_image_arg(args)
    for gbuild in args.genomes:
        print("Preparing %s" % gbuild)
        if args.prepped:
            for target in ["samtools"] + args.aligners:
                genome.download_prepped_genome(gbuild, {}, target, False,
                                               args.prepped)
            print("Downloaded prepped %s to %s. Edit and re-run without "
                  "--prepped to upload" % (gbuild, args.prepped))
            return
        cmdline = ["upgrade", "--genomes", gbuild]
        for aligner in args.aligners:
            cmdline += ["--aligners", aligner]
        dmounts = mounts.prepare_system(args.datadir,
                                        constant.DOCKER['biodata_dir'])
        manage.run_bcbio_cmd(args.image, dmounts, cmdline)
        print("Uploading %s" % gbuild)
        gdir = _get_basedir(args.datadir, gbuild)
        basedir, genomedir = os.path.split(gdir)
        assert genomedir == gbuild
        with utils.chdir(basedir):
            all_dirs = sorted(os.listdir(gbuild))
            _upload_biodata(gbuild, "seq", all_dirs)
            for aligner in args.aligners:
                _upload_biodata(
                    gbuild, genome.REMAP_NAMES.get(aligner, aligner), all_dirs)


def _upload_biodata(gbuild, target, all_dirs):
    """Upload biodata for a specific genome build and target to S3.
    """
    if target == "seq":
        want_dirs = set(["rnaseq", "seq", "variation", "vep", "snpeff"])
        target_dirs = [x for x in all_dirs
                       if (x.startswith("rnaseq-") or x in want_dirs)]
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
               "-m x-amz-storage-class:REDUCED_REDUNDANCY -m "
               "x-amz-acl:public-read")
        do.run(cmd.format(**locals()),
               "Upload pre-prepared genome data: %s %s" % (gbuild, target))


def _get_basedir(datadir, target_genome):
    """Retrieve base directory for uploading.
    """
    genome_dir = os.path.join(datadir, "genomes")
    for dirname in glob.glob(os.path.join(genome_dir, "*", "*")):
        if dirname.endswith("/%s" % target_genome):
            return dirname
