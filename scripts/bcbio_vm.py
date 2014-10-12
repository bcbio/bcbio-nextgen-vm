#!/usr/bin/env python -E
"""Run and install bcbio-nextgen, using code and tools isolated in a docker container.

Work in progress script to explore the best ways to integrate docker isolated
software with external data.
"""
from __future__ import print_function
import argparse
import os
import sys

import yaml

import warnings
warnings.simplefilter("ignore", UserWarning, 1155)  # Stop warnings from matplotlib.use()

from bcbio.distributed import clargs
from bcbio.pipeline import main
from bcbiovm.aws import iam, icel, vpc
from bcbiovm.docker import defaults, install, manage, mounts, run
from bcbiovm.clusterk import main as clusterk_main
from bcbiovm.ship import pack

# default information about docker container
DOCKER = {"port": 8085,
          "biodata_dir": "/usr/local/share/bcbio-nextgen",
          "input_dir": "/mnt/inputs",
          "work_dir": "/mnt/work",
          "image_url": "https://s3.amazonaws.com/bcbio_nextgen/bcbio-nextgen-docker-image.gz"}

def cmd_install(args):
    args = defaults.update_check_args(args, "bcbio-nextgen not upgraded.",
                                      need_datadir=args.install_data)
    install.full(args, DOCKER)

def cmd_run(args):
    args = defaults.update_check_args(args, "Could not run analysis.")
    args = install.docker_image_arg(args)
    run.do_analysis(args, DOCKER)

def cmd_ipython(args):
    args = defaults.update_check_args(args, "Could not run IPython parallel analysis.")
    args = install.docker_image_arg(args)
    parallel = clargs.to_parallel(args, "bcbiovm.docker")
    parallel["wrapper"] = "runfn"
    with open(args.sample_config) as in_handle:
        ready_config, _ = mounts.normalize_config(yaml.load(in_handle), args.fcdir)
    work_dir = os.getcwd()
    ready_config_file = os.path.join(work_dir, "%s-ready%s" %
                                     (os.path.splitext(os.path.basename(args.sample_config))))
    with open(ready_config_file, "w") as out_handle:
        yaml.safe_dump(ready_config, out_handle, default_flow_style=False, allow_unicode=False)
    work_dir = os.getcwd()
    systemconfig = run.local_system_config(args.systemconfig, args.datadir, work_dir)
    cur_pack = pack.shared_filesystem(work_dir, args.datadir, args.tmpdir)
    parallel["wrapper_args"] = [DOCKER, {"sample_config": ready_config_file,
                                         "fcdir": args.fcdir,
                                         "pack": cur_pack,
                                         "systemconfig": systemconfig,
                                         "image": args.image}]
    # For testing, run on a local ipython cluster
    parallel["run_local"] = parallel.get("queue") == "localrun"
    workdir_mount = "%s:%s" % (work_dir, DOCKER["work_dir"])
    manage.run_bcbio_cmd(args.image, [workdir_mount],
                         ["version", "--workdir=%s" % DOCKER["work_dir"]])
    cmd_args = {"systemconfig": systemconfig, "image": args.image, "pack": cur_pack,
                "sample_config": args.sample_config, "fcdir": args.fcdir,
                "orig_systemconfig": args.systemconfig}
    config = {"algorithm": {}, "resources": {}}
    runargs = [ready_config_file, systemconfig, work_dir, args.fcdir, config]
    samples = run.do_runfn("organize_samples", runargs, cmd_args, parallel, DOCKER)
    main.run_main(work_dir, run_info_yaml=ready_config_file,
                  config_file=systemconfig, fc_dir=args.fcdir,
                  parallel=parallel, samples=samples)

def cmd_clusterk(args):
    args = defaults.update_check_args(args, "Could not run Clusterk parallel analysis.")
    args = install.docker_image_arg(args)
    clusterk_main.run(args, DOCKER)

def cmd_runfn(args):
    args = defaults.update_check_args(args, "Could not run bcbio-nextgen function.")
    args = install.docker_image_arg(args)
    with open(args.parallel) as in_handle:
        parallel = yaml.safe_load(in_handle)
    with open(args.runargs) as in_handle:
        runargs = yaml.safe_load(in_handle)
    cmd_args = {"systemconfig": args.systemconfig, "image": args.image, "pack": parallel["pack"]}
    out = run.do_runfn(args.fn_name, runargs, cmd_args, parallel, DOCKER)
    out_file = "%s-out%s" % os.path.splitext(args.runargs)
    with open(out_file, "w") as out_handle:
        yaml.safe_dump(out, out_handle, default_flow_style=False, allow_unicode=False)
    pack.send_output(parallel["pack"], out_file)

def cmd_server(args):
    args = defaults.update_check_args(args, "Could not run server.")
    args = install.docker_image_arg(args)
    ports = ["%s:%s" % (args.port, DOCKER["port"])]
    print("Running server on port %s. Press ctrl-c to exit." % args.port)
    manage.run_bcbio_cmd(args.image, [], ["server", "--port", str(DOCKER["port"])],
                         ports)

def cmd_save_defaults(args):
    defaults.save(args)

def _install_cmd(subparsers, name):
    parser_i = subparsers.add_parser(name, help="Install or upgrade bcbio-nextgen docker container and data.")
    parser_i.add_argument("--genomes", help="Genomes to download",
                          action="append", default=[],
                          choices=["GRCh37", "hg19", "mm10", "mm9", "rn5", "canFam3", "dm3", "Zv9", "phix",
                                   "sacCer3", "xenTro3", "TAIR10", "WBcel235"])
    parser_i.add_argument("--aligners", help="Aligner indexes to download",
                          action="append", default=[],
                          choices=["bowtie", "bowtie2", "bwa", "novoalign", "star", "ucsc"])
    parser_i.add_argument("--data", help="Install or upgrade data dependencies",
                          dest="install_data", action="store_true", default=False)
    parser_i.add_argument("--tools", help="Install or upgrade tool dependencies",
                          dest="install_tools", action="store_true", default=False)
    parser_i.add_argument("--wrapper", help="Update wrapper bcbio-nextgen-vm code",
                          action="store_true", default=False)
    parser_i.add_argument("--image", help="Docker image name to use, could point to compatible pre-installed image.",
                          default=None)
    parser_i.set_defaults(func=cmd_install)

def _std_config_args(parser):
    parser.add_argument("--systemconfig", help="Global YAML configuration file specifying system details. "
                        "Defaults to installed bcbio_system.yaml.")
    parser.add_argument("-n", "--numcores", help="Total cores to use for processing",
                        type=int, default=1)
    return parser

def _std_run_args(parser):
    parser.add_argument("sample_config", help="YAML file with details about samples to process.")
    parser.add_argument("--fcdir", help="A directory of Illumina output or fastq files to process",
                        type=lambda x: (os.path.abspath(os.path.expanduser(x))))
    parser = _std_config_args(parser)
    return parser

def _run_cmd(subparsers):
    parser_r = subparsers.add_parser("run", help="Run an automated analysis on the local machine.")
    parser_r = _std_run_args(parser_r)
    parser_r.set_defaults(func=cmd_run)

def _run_ipython_cmd(subparsers):
    parser = subparsers.add_parser("ipython", help="Run on a cluster using IPython parallel.")
    parser = _std_run_args(parser)
    parser.add_argument("scheduler", help="Scheduler to use.", choices=["lsf", "sge", "torque", "slurm"])
    parser.add_argument("queue", help="Scheduler queue to run jobs on.")
    parser.add_argument("-r", "--resources",
                        help=("Cluster specific resources specifications. Can be specified multiple times.\n"
                              "Supports SGE and SLURM parameters."),
                        default=[], action="append")
    parser.add_argument("--timeout", help="Number of minutes before cluster startup times out. Defaults to 15",
                        default=15, type=int)
    parser.add_argument("--retries",
                        help=("Number of retries of failed tasks during distributed processing. "
                              "Default 0 (no retries)"),
                        default=0, type=int)
    parser.add_argument("-t", "--tag", help="Tag name to label jobs on the cluster",
                        default="")
    parser.add_argument("--tmpdir", help="Path of local on-machine temporary directory to process in.")
    parser.set_defaults(func=cmd_ipython)

def _runfn_cmd(subparsers):
    parser = subparsers.add_parser("runfn", help="Run a specific bcbio-nextgen function with provided arguments")
    parser = _std_config_args(parser)
    parser.add_argument("fn_name", help="Name of the function to run")
    parser.add_argument("parallel", help="JSON/YAML file describing the parallel environment")
    parser.add_argument("runargs", help="JSON/YAML file with arguments to the function")
    parser.set_defaults(func=cmd_runfn)

def _run_clusterk_cmd(subparsers):
    parser = subparsers.add_parser("clusterk", help="Run on Amazon web services using Clusterk.")
    parser = _std_run_args(parser)
    parser.add_argument("run_bucket", help="Name of the S3 bucket to use for storing run information")
    parser.add_argument("biodata_bucket", help="Name of the S3 bucket to use for storing biodata like genomes")
    parser.add_argument("-q", "--queue", help="Clusterk queue to run jobs on.", default="default")
    parser.set_defaults(func=cmd_clusterk)

def _server_cmd(subparsers):
    parser_s = subparsers.add_parser("server", help="Persistent REST server receiving requests via the specified port.")
    parser_s.add_argument("--port", default=8085, help="External port to connect to docker image.")
    parser_s.set_defaults(func=cmd_server)

def _config_cmd(subparsers):
    parser_c = subparsers.add_parser("saveconfig", help="Save standard configuration variables for current user. "
                                     "Avoids need to specify on the command line in future runs.")
    parser_c.set_defaults(func=cmd_save_defaults)

def _aws_cmd(subparsers):
    parser_c = subparsers.add_parser("aws", help="Automate resources for running bcbio on AWS")
    awssub = parser_c.add_subparsers(title="[aws commands]")

    _aws_iam_cmd(awssub)
    _aws_icel_cmd(awssub)
    _aws_vpc_cmd(awssub)

def _aws_iam_cmd(awsparser):
    parser = awsparser.add_parser("iam", help="Create IAM user and policies")
    parser.add_argument("--recreate", action="store_true", default=False,
                        help="Recreate current IAM user access keys")
    parser.set_defaults(func=iam.bootstrap)

def _aws_vpc_cmd(awsparser):
    parser = awsparser.add_parser("vpc",
                                  help="Create VPC and associated resources",
                                  formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--recreate", action="store_true", default=False,
                        help="Remove and recreate the VPC, destroying all "
                             "AWS resources contained in it")
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")
    parser.add_argument("-n", "--network", default="10.0.0.0/16",
                        help="network to use for the VPC, "
                             "in CIDR notation (a.b.c.d/e)")
    parser.set_defaults(func=vpc.bootstrap)

def _aws_icel_cmd(awsparser):
    parser_c = awsparser.add_parser("icel",
                                    help="Create Lustre scratch filesystem "
                                         "using Intel Cloud Edition for "
                                         "Lustre")
    icel_parser = parser_c.add_subparsers(title="[icel create]")

    parser = icel_parser.add_parser("create",
                                    help="Create Lustre scratch filesystem "
                                         "using Intel Cloud Edition for "
                                         "Lustre",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--recreate", action="store_true", default=False,
                        help="Remove and recreate the stack, "
                             "destroying all data stored on it")
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")

    parser.add_argument("-s", "--size", type=int, default="2048",
                        help="Size of the Lustre filesystem, in gigabytes")
    parser.add_argument("-o", "--oss-count", type=int, default="4",
                        help="Number of OSS nodes")
    parser.add_argument("-l", "--lun-count", type=int, default="4",
                        help="Number of EBS LUNs per OSS")

    parser.add_argument("-n", "--network", metavar="NETWORK", dest="network",
                        help="Network (in CIDR notation, a.b.c.d/e) to "
                             "place Lustre servers in")

    parser.add_argument(metavar="STACK_NAME", dest="stack_name", nargs="?",
                        default="bcbiolustre",
                        help="CloudFormation name for the new stack")
    parser.set_defaults(func=icel.create)


    parser = icel_parser.add_parser("fs_spec",
                                     help="Get the filesystem spec for a "
                                          "running filesystem",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")
    parser.add_argument(metavar="STACK_NAME", dest="stack_name", nargs="?",
                        default="bcbiolustre",
                        help="CloudFormation name for the stack")
    parser.set_defaults(func=icel.fs_spec)


    parser = icel_parser.add_parser("mount",
                                     help="Mount Lustre filesystem on "
                                          "all cluster nodes",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Emit verbose output when running "
                             "Ansible playbooks")
    parser.add_argument(metavar="STACK_NAME", dest="stack_name", nargs="?",
                        default="bcbiolustre",
                        help="CloudFormation name for the new stack")
    parser.set_defaults(func=icel.mount)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Automatic installation for bcbio-nextgen pipelines, with docker.")
    parser.add_argument("--datadir", help="Directory with genome data and associated files.",
                        type=lambda x: (os.path.abspath(os.path.expanduser(x))))
    subparsers = parser.add_subparsers(title="[sub-commands]")
    _run_cmd(subparsers)
    _install_cmd(subparsers, name="install")
    _install_cmd(subparsers, name="upgrade")
    _run_ipython_cmd(subparsers)
    _run_clusterk_cmd(subparsers)
    _aws_cmd(subparsers)
    _server_cmd(subparsers)
    _runfn_cmd(subparsers)
    _config_cmd(subparsers)
    if len(sys.argv) == 1:
        parser.print_help()
    else:
        args = parser.parse_args()
        args.func(args)
