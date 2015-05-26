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


import warnings
warnings.simplefilter("ignore", UserWarning, 1155)  # Stop warnings from matplotlib.use()

from bcbio.workflow import template
from bcbiovm.aws import common
from bcbiovm.docker import defaults, devel


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


def _template_cmd(subparsers):
    parser = subparsers.add_parser("template",
                                   help="Create a bcbio sample.yaml file from a standard template and inputs")
    parser = template.setup_args(parser)
    parser.add_argument('--relpaths', help="Convert inputs into relative paths to the work directory",
                        action='store_true', default=False)
    parser.set_defaults(func=template.setup)


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
