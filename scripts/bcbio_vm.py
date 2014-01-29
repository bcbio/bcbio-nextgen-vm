#!/usr/bin/env python
"""Run and install bcbio-nextgen, using code and tools isolated in a docker container.

Work in progress script to explore the best ways to integrate docker isolated
software with external data.
"""
from __future__ import print_function
import argparse
import os
import sys

from bcbio.distributed import clargs
from bcbio.pipeline import main
from bcbiovm.docker import defaults, install, manage, run

# default information about docker container
DOCKER = {"port": 8085,
          "biodata_dir": "/mnt/biodata",
          "input_dir": "/mnt/inputs",
          "work_dir": "/mnt/work",
          "image": "chapmanb/bcbio-nextgen-devel",
          "image_url": "https://s3.amazonaws.com/bcbio_nextgen/bcbio-nextgen-docker-image.gz"}

def cmd_install(args):
    args = defaults.update_check_args(args, "bcbio-nextgen not upgraded.")
    install.full(args, DOCKER)

def cmd_run(args):
    args = defaults.update_check_args(args, "Could not run analysis.")
    run.do_analysis(args, DOCKER)

def cmd_ipython(args):
    args = defaults.update_check_args(args, "Could not run IPython parallel analysis.")
    parallel = clargs.to_parallel(args, "bcbiovm.docker")
    parallel["wrapper"] = "runfn"
    parallel["wrapper_args"] = [DOCKER, {"sample_config": args.sample_config,
                                         "fcdir": args.fcdir,
                                         "datadir": args.datadir,
                                         "systemconfig": args.systemconfig}]
    parallel["run_local"] = True
    work_dir = os.getcwd()
    main.run_main(work_dir, run_info_yaml=args.sample_config,
                  config_file=args.systemconfig, fc_dir=args.fcdir,
                  parallel=parallel)

def cmd_server(args):
    args = defaults.update_check_args(args, "Could not run server.")
    ports = ["%s:%s" % (args.port, DOCKER["port"])]
    print("Running server on port %s. Press ctrl-c to exit." % args.port)
    manage.run_bcbio_cmd(DOCKER["image"], [], "server --port %s" % DOCKER["port"],
                         ports)

def cmd_save_defaults(args):
    defaults.save(args)

def _install_cmd(subparsers):
    parser_i = subparsers.add_parser("install", help="Install or upgrade bcbio-nextgen docker container and data.")
    parser_i.add_argument("--genomes", help="Genomes to download",
                          action="append", default=["GRCh37"],
                          choices=["GRCh37", "hg19", "mm10", "mm9", "rn5", "canFam3"])
    parser_i.add_argument("--aligners", help="Aligner indexes to download",
                          action="append", default=["bwa"],
                          choices=["bowtie", "bowtie2", "bwa", "novoalign", "ucsc"])
    parser_i.add_argument("--data", help="Install or upgrade data dependencies",
                          dest="install_data", action="store_true", default=False)
    parser_i.add_argument("--tools", help="Install or upgrade tool dependencies",
                          dest="install_tools", action="store_true", default=False)
    parser_i.add_argument("--inplace", help="Perform an in-place update of code and tools.",
                          action="store_true", default=False)
    parser_i.add_argument("--wrapper", help="Update wrapper bcbio-nextgen-vm code",
                          action="store_true", default=False)
    parser_i.set_defaults(func=cmd_install)

def _std_run_args(parser):
    parser.add_argument("sample_config", help="YAML file with details about samples to process.")
    parser.add_argument("--fcdir", help="A directory of Illumina output or fastq files to process",
                        type=lambda x: (os.path.abspath(os.path.expanduser(x))))
    parser.add_argument("--systemconfig", help="Global YAML configuration file specifying system details. "
                        "Defaults to installed bcbio_system.yaml.")
    parser.add_argument("-n", "--numcores", help="Total cores to use for processing",
                        type=int, default=1)
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
    parser.set_defaults(func=cmd_ipython)

def _server_cmd(subparsers):
    parser_s = subparsers.add_parser("server", help="Persistent REST server receiving requests via the specified port.")
    parser_s.add_argument("--port", default=8085, help="External port to connect to docker image.")
    parser_s.set_defaults(func=cmd_server)

def _config_cmd(subparsers):
    parser_c = subparsers.add_parser("saveconfig", help="Save standard configuration variables for current user. "
                                     "Avoids need to specify on the command line in future runs.")
    parser_c.set_defaults(func=cmd_save_defaults)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Automatic installation for bcbio-nextgen pipelines, with docker.")
    parser.add_argument("--datadir", help="Directory with genome data and associated files.",
                        type=lambda x: (os.path.abspath(os.path.expanduser(x))))
    subparsers = parser.add_subparsers(title="[sub-commands]")
    _install_cmd(subparsers)
    _run_cmd(subparsers)
    _run_ipython_cmd(subparsers)
    _server_cmd(subparsers)
    _config_cmd(subparsers)
    if len(sys.argv) == 1:
        parser.print_help()
    else:
        args = parser.parse_args()
        args.func(args)
