#!/usr/bin/env python -E
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

import yaml

import warnings
warnings.simplefilter("ignore", UserWarning, 1155)  # Stop warnings from matplotlib.use()

from bcbio.distributed import clargs
from bcbio.workflow import template
from bcbiovm.aws import common
from bcbiovm.docker import defaults, devel, install, manage, mounts, run
from bcbiovm.ipython import batchprep
from bcbiovm.ship import pack


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


def cmd_server(args):
    args = defaults.update_check_args(args, "Could not run server.")
    args = install.docker_image_arg(args)
    ports = ["%s:%s" % (args.port, devel.DOCKER["port"])]
    print("Running server on port %s. Press ctrl-c to exit." % args.port)
    manage.run_bcbio_cmd(args.image, [], ["server", "--port", str(devel.DOCKER["port"])],
                         ports)


def cmd_save_defaults(args):
    defaults.save(args)


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
    parser.add_argument('--relpaths', help="Convert inputs into relative paths to the work directory",
                        action='store_true', default=False)
    parser.set_defaults(func=template.setup)


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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Automatic installation for bcbio-nextgen pipelines, with docker.")
    parser.add_argument("--datadir", help="Directory with genome data and associated files.",
                        type=lambda x: (os.path.abspath(os.path.expanduser(x))))
    subparsers = parser.add_subparsers(title="[sub-commands]")
    _run_cmd(subparsers)
    _run_ipython_cmd(subparsers)
    _run_ipythonprep_cmd(subparsers)
    _template_cmd(subparsers)
    _elasticluster_cmd(subparsers)
    # _server_cmd(subparsers)
    devel.setup_cmd(subparsers)
    _config_cmd(subparsers)
    if len(sys.argv) == 1:
        parser.print_help()
    else:
        if len(sys.argv) > 1 and sys.argv[1] == "elasticluster":
            sys.exit(common.wrap_elasticluster(sys.argv[1:]))
        else:
            args = parser.parse_args()
            args.func(args)
