#!/usr/bin/env python
"""Run and install bcbio-nextgen, using code and tools isolated in a docker container.

Work in progress script to explore the best ways to integrate docker isolated
software with external data.
"""
from __future__ import print_function
from __future__ import unicode_literals
import argparse
import os
import sys

from bcbiovm.docker import defaults, install, run

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

def cmd_save_defaults(args):
    defaults.save(args)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Automatic installation for bcbio-nextgen pipelines, with docker.")
    parser.add_argument("--port", default=8085, help="External port to connect to docker image.")
    parser.add_argument("--datadir", help="Directory to install genome data and associated files.",
                        type=lambda x: (os.path.abspath(os.path.expanduser(x))))
    parser.add_argument("--develrepo", help=("Specify a development repository to link. "
                                             "Used for debugging and development"))
    subparsers = parser.add_subparsers(title="[sub-commands]")
    # installation
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
    parser_i.set_defaults(func=cmd_install)
    # running
    parser_r = subparsers.add_parser("run", help="Run an automated analysis.")
    parser_r.add_argument("sample_config", help="YAML file with details about samples to process.")
    parser_r.add_argument("--fcdir", help="A directory of Illumina output or fastq files to process",
                          type=lambda x: (os.path.abspath(os.path.expanduser(x))))
    parser_r.add_argument("--systemconfig", help="Global YAML configuration file specifying system details. "
                          "Defaults to installed bcbio_system.yaml.")
    parser_r.add_argument("-n", "--numcores", help="Total cores to use for processing",
                          type=int, default=1)
    parser_r.set_defaults(func=cmd_run)
    # configuration
    parser_c = subparsers.add_parser("saveconfig", help="Save standard configuration variables for current user. "
                                     "Avoids need to specify on the command line in future runs.")
    parser_c.set_defaults(func=cmd_save_defaults)
    if len(sys.argv) == 1:
        parser.print_help()
    else:
        args = parser.parse_args()
        args.func(args)
