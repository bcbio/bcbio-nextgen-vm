#!/usr/bin/env python -Es
"""Run and install bcbio-nextgen, using code and tools isolated in a docker container.

See the bcbio documentation https://bcbio-nextgen.readthedocs.org for more details
about running it for analysis.

This script builds the command line options for bcbio_vm.py, which you can see by
running `bcbio_vm.py -h`. For each specific command, like `install`, we'll have a function to
prepare the command line arguments (`_install_cmd`) and a function to do the actual
work (`cmd_install`).
"""
from __future__ import print_function
import argparse
import os
import sys
import warnings

import yaml

from bcbio.cwl import main as cwl_main
from bcbio.cwl import tool as cwl_tool
from bcbio.distributed import clargs
from bcbio.workflow import template
from bcbiovm.arvados import retriever as arvados_retriever
from bcbiovm.dnanexus import retriever as dx_retriever
from bcbiovm.sbgenomics import retriever as sb_retriever
from bcbiovm.gcp import retriever as gs_retriever
from bcbiovm.aws import (ansible_inputs, cluster, common, iam, icel, vpc, info,
                         s3retriever, cromwell)
from bcbiovm.docker import defaults, devel, install, manage, mounts, run
from bcbiovm.ipython import batchprep
from bcbiovm.shared import localref
from bcbiovm.ship import pack

warnings.simplefilter("ignore", UserWarning, 1155)  # Stop warnings from matplotlib.use()

def cmd_install(args):
    args = defaults.update_check_args(args, "bcbio-nextgen not upgraded.",
                                      need_datadir=args.install_data)
    install.full(args, devel.DOCKER)

def cmd_run(args):
    args = defaults.update_check_args(args, "Could not run analysis.")
    args = install.docker_image_arg(args)
    run.do_analysis(args, devel.DOCKER)

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
    parallel["wrapper_args"] = [devel.DOCKER, {"sample_config": ready_config_file,
                                               "fcdir": args.fcdir,
                                               "pack": cur_pack,
                                               "systemconfig": systemconfig,
                                               "image": args.image}]
    # For testing, run on a local ipython cluster
    parallel["run_local"] = parallel.get("queue") == "localrun"

    from bcbio.pipeline import main
    main.run_main(work_dir, run_info_yaml=ready_config_file,
                  config_file=systemconfig, fc_dir=args.fcdir,
                  parallel=parallel)

    # Approach for running main function inside of docker
    # Could be useful for architectures where we can spawn docker jobs from docker
    #
    # cmd_args = {"systemconfig": systemconfig, "image": args.image, "pack": cur_pack,
    #             "sample_config": args.sample_config, "fcdir": args.fcdir,
    #             "orig_systemconfig": args.systemconfig}
    # main_args = [work_dir, ready_config_file, systemconfig, args.fcdir, parallel]
    # run.do_runfn("run_main", main_args, cmd_args, parallel, devel.DOCKER)

def cmd_clusterk(args):
    args = defaults.update_check_args(args, "Could not run Clusterk parallel analysis.")
    args = install.docker_image_arg(args)
    clusterk_main.run(args, devel.DOCKER)

def cmd_runfn(args):
    args = defaults.update_check_args(args, "Could not run bcbio-nextgen function.")
    args = install.docker_image_arg(args)
    with open(args.parallel) as in_handle:
        parallel = yaml.safe_load(in_handle)
    with open(args.runargs) as in_handle:
        runargs = yaml.safe_load(in_handle)
    cmd_args = {"systemconfig": args.systemconfig, "image": args.image, "pack": parallel["pack"]}
    out = run.do_runfn(args.fn_name, runargs, cmd_args, parallel, devel.DOCKER)
    out_file = "%s-out%s" % os.path.splitext(args.runargs)
    with open(out_file, "w") as out_handle:
        yaml.safe_dump(out, out_handle, default_flow_style=False, allow_unicode=False)
    pack.send_output(parallel["pack"], out_file)

def cmd_server(args):
    args = defaults.update_check_args(args, "Could not run server.")
    args = install.docker_image_arg(args)
    ports = ["%s:%s" % (args.port, devel.DOCKER["port"])]
    print("Running server on port %s. Press ctrl-c to exit." % args.port)
    manage.run_bcbio_cmd(args.image, [], ["server", "--port", str(devel.DOCKER["port"])],
                         ports)

def cmd_save_defaults(args):
    defaults.save(args)

def _install_cmd(subparsers, name):
    parser_i = subparsers.add_parser(name, help="Install or upgrade bcbio-nextgen docker container and data.")
    parser_i = devel.add_biodata_args(parser_i)
    parser_i.add_argument("--data", help="Install or upgrade data dependencies",
                          dest="install_data", action="store_true", default=False)
    parser_i.add_argument("--tools", help="Install or upgrade tool dependencies",
                          dest="install_tools", action="store_true", default=False)
    parser_i.add_argument("--wrapper", help="Update wrapper bcbio-nextgen-vm code",
                          action="store_true", default=False)
    parser_i.add_argument("--image", help="Docker image name to use, could point to compatible pre-installed image.",
                          default=None)
    parser_i.add_argument("--cores", help="Cores to use for parallel data prep processes", default=1, type=int)
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
    parser.add_argument("--image", help="Docker image name to use, could point to compatible pre-installed image.",
                        default=None)
    parser = _std_config_args(parser)
    return parser

def _run_cmd(subparsers):
    parser_r = subparsers.add_parser("run", help="Run an automated analysis on the local machine.")
    parser_r = _std_run_args(parser_r)
    parser_r.set_defaults(func=cmd_run)

def _cwl_cmd(subparsers):
    parser = subparsers.add_parser("cwl", help="Generate Common Workflow Language (CWL) from configuration inputs")
    parser.add_argument("--systemconfig", help="Global YAML configuration file specifying system details. "
                        "Defaults to installed bcbio_system.yaml.")
    parser.add_argument("sample_config", help="YAML file with details about samples to process.")
    parser.add_argument('--add-container-tag',
                        help="Add a container revision tag to CWL ('quay_lookup` retrieves lates from quay.io)",
                        default=None)
    parser.set_defaults(integrations={"arvados": arvados_retriever, "s3": s3retriever, "sbgenomics": sb_retriever,
                                      "dnanexus": dx_retriever, "gs": gs_retriever, "local": localref})
    parser.set_defaults(func=cwl_main.run)

def _cwlrun_cmd(subparsers):
    parser = subparsers.add_parser("cwlrun", help="Run Common Workflow Language (CWL) inputs with a specified tool")
    parser.add_argument("tool", help="CWL tool to run", choices=["cwltool", "arvados", "toil", "bunny", "funnel",
                                                                 "cromwell", "sbg", "wes"])
    parser.add_argument("directory", help="Directory with bcbio generated CWL")
    parser.add_argument("toolargs", help="Arguments to pass to CWL tool", nargs="*")
    parser.add_argument('--no-container', help="Use local installation of bcbio instead of Docker container",
                        action='store_true', default=False)
    parser.add_argument("-s", "--scheduler",
                        choices=["lsf", "sge", "torque", "slurm", "pbspro", "htcondor"],
                        help="Scheduler to use, for an HPC system")
    parser.add_argument("-q", "--queue",
                        help=("Scheduler queue to run jobs on, for an HPC system"))
    parser.add_argument("-r", "--resources",
                        help=("Cluster specific resources specifications. "
                              "Can be specified multiple times.\n"
                              "Supports SGE, Torque, LSF and SLURM "
                              "parameters."), default=[], action="append")
    parser.add_argument("-j", "--joblimit", type=int, default=0,
                        help=("Maximum number of simultaneous jobs (not cores) submitted. "
                              "Only supported for Cromwell runner. "
                              "Defaults to 1 for local runner, unlimited otherwise."))
    parser.add_argument("--runconfig", help=("Custom configuration HOCON file for Cromwell."))
    parser.add_argument("--cloud-project", help=("Remote cloud project for running jobs. Cromwell GCP support."))
    parser.add_argument("--cloud-root", help=("Remote bucket location for run files. Cromwell GCP support."))
    parser.add_argument("--host", help=("WES: host for submitting jobs"))
    parser.add_argument("--auth", help=("WES: authentication token"))
    parser.set_defaults(func=cwl_tool.run)

def _add_ipython_args(parser):
    parser = _std_run_args(parser)
    parser.add_argument("scheduler", help="Scheduler to use.", choices=["lsf", "sge", "torque", "slurm", "pbspro"])
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
    return parser

def _run_ipython_cmd(subparsers):
    parser = subparsers.add_parser("ipython", help="Run on a cluster using IPython parallel.")
    parser = _add_ipython_args(parser)
    parser.set_defaults(func=cmd_ipython)

def _run_ipythonprep_cmd(subparsers):
    parser = subparsers.add_parser("ipythonprep", help="Prepare a batch script to run bcbio on a scheduler.")
    parser = _add_ipython_args(parser)
    parser.set_defaults(func=batchprep.submit_script)

def _template_cmd(subparsers):
    parser = subparsers.add_parser("template",
                                   help="Create a bcbio sample.yaml file from a standard template and inputs")
    parser = template.setup_args(parser)
    parser = _std_config_args(parser)
    parser.add_argument('--relpaths', help="Convert inputs into relative paths to the work directory",
                        action='store_true', default=False)
    parser.set_defaults(integrations={"arvados": arvados_retriever, "s3": s3retriever, "sbgenomics": sb_retriever,
                                      "dnanexus": dx_retriever, "gs": gs_retriever, "local": localref})
    parser.set_defaults(func=template.setup)

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

def _elasticluster_cmd(subparsers):
    subparsers.add_parser("elasticluster", help="Interface to standard elasticluster commands")

def _graph_cmd(subparsers):
    from bcbiovm.graph import graph
    parser = subparsers.add_parser("graph",
                                   help="Generate system graphs "
                                        "(CPU/memory/network/disk I/O "
                                        "consumption) from bcbio runs",
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("log",
                        help="Local path to bcbio log file written by the run.")
    parser.add_argument("-o", "--outdir", default="monitoring/graphs",
                        help="Directory to write graphs to.")
    parser.add_argument("-r", "--rawdir", default="monitoring/collectl",
                        help="Directory to put raw collectl data files.")
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")
    parser.add_argument("-e", "--econfig",
                        help="Elasticluster bcbio configuration file",
                        default=common.DEFAULT_EC_CONFIG)
    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="Emit verbose output")
    parser.add_argument("-s", "--serialize", action="store_true", default=False,
                        help="Serialize plot information for later faster inspection")
    parser.set_defaults(func=graph.bootstrap)

def _aws_cmd(subparsers):
    parser_c = subparsers.add_parser("aws", help="Automate resources for running bcbio on AWS")
    awssub = parser_c.add_subparsers(title="[aws commands]")
    cromwell.setup_cmd(awssub)
    cluster.setup_cmd(awssub)
    info.setup_cmd(awssub)
    ansible_inputs.setup_cmd(awssub)
    _aws_iam_cmd(awssub)
    _aws_vpc_cmd(awssub)
    icel.setup_cmd(awssub)

def _aws_iam_cmd(awsparser):
    parser = awsparser.add_parser("iam", help="Create IAM user and policies")
    parser.add_argument("--econfig", help="Elasticluster bcbio configuration file",
                        default=common.DEFAULT_EC_CONFIG)
    parser.add_argument("--region", help="EC2 region to create IAM user in (defaults to us-east-1)",
                        default="us-east-1")
    parser.add_argument("--recreate", action="store_true", default=False,
                        help="Recreate current IAM user access keys")
    parser.add_argument("--nocreate", action="store_true", default=False,
                        help=("Do not create a new IAM user, just generate a configuration file. "
                              "Useful for users without full permissions to IAM."))
    parser.set_defaults(func=iam.bootstrap)

def _aws_vpc_cmd(awsparser):
    parser = awsparser.add_parser("vpc",
                                  help="Create VPC and associated resources",
                                  formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--econfig", help="Elasticluster bcbio configuration file",
                        default=common.DEFAULT_EC_CONFIG)
    parser.add_argument("--region", help="EC2 region to create VPC in (defaults to us-east-1)",
                        default="us-east-1")
    parser.add_argument("--recreate", action="store_true", default=False,
                        help="Remove and recreate the VPC, destroying all "
                             "AWS resources contained in it")
    parser.add_argument("-c", "--cluster", default="bcbio",
                        help="elasticluster cluster name")
    parser.add_argument("-n", "--network", default="10.0.0.0/16",
                        help="network to use for the VPC, "
                             "in CIDR notation (a.b.c.d/e)")
    parser.set_defaults(func=vpc.bootstrap)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Automatic installation for bcbio-nextgen pipelines, with docker.")
    parser.add_argument("--datadir", help="Directory with genome data and associated files.",
                        type=lambda x: (os.path.abspath(os.path.expanduser(x))))
    subparsers = parser.add_subparsers(title="[sub-commands]")
    _template_cmd(subparsers)
    _cwl_cmd(subparsers)
    _cwlrun_cmd(subparsers)
    _install_cmd(subparsers, name="install")
    _install_cmd(subparsers, name="upgrade")
    _run_cmd(subparsers)
    _run_ipython_cmd(subparsers)
    _run_ipythonprep_cmd(subparsers)
    #_run_clusterk_cmd(subparsers)
    # _server_cmd(subparsers)
    _runfn_cmd(subparsers)
    devel.setup_cmd(subparsers)
    _aws_cmd(subparsers)
    _elasticluster_cmd(subparsers)
    #_graph_cmd(subparsers)
    _config_cmd(subparsers)
    if len(sys.argv) == 1:
        parser.print_help()
    else:
        if len(sys.argv) > 1 and sys.argv[1] == "elasticluster":
            sys.exit(common.wrap_elasticluster(sys.argv[1:]))
        else:
            args = parser.parse_args()
            args.func(args)
