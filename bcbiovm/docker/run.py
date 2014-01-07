"""Run a bcbio-nextgen analysis inside of an isolated docker container.
"""
from __future__ import print_function
from __future__ import unicode_literals
from future.builtins import open

import json
import os
import subprocess
import sys

import requests
import yaml

from bcbiovm.docker import manage, mounts

def do_analysis(args, dockerconf):
    """Run a full analysis on a local machine, utilizing multiple cores.
    """
    with open(args.sample_config) as in_handle:
        sample_config, dmounts = mounts.update_config(args, yaml.load(in_handle), dockerconf["input_dir"])
    dmounts += mounts.prepare_system(args.datadir, dockerconf["biodata_dir"])
    dmounts.append("%s:%s" % (os.getcwd(), dockerconf["work_dir"]))
    system_config, system_mounts = read_system_config(args, dockerconf)
    with manage.bcbio_docker(dockerconf, dmounts + system_mounts, args) as cid:
        print("Running analysis using docker container: %s" % cid)
        payload = {"work_dir": dockerconf["work_dir"],
                   "system_config": system_config,
                   "sample_config": sample_config,
                   "numcores": args.numcores}
        r = requests.get("http://localhost:{port}/run".format(port=args.port), params={"args": json.dumps(payload)})
        run_id = r.text
        p = subprocess.Popen(["docker", "logs", "-f", cid], bufsize=1,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # monitor processing status, writing logging information
        for log_info in iter(p.stdout.readline, ""):
            print(log_info.rstrip())
            r = requests.get("http://localhost:{port}/status".format(port=args.port), params={"run_id": run_id})
            if r.text != "running":
                if log_info.rstrip().endswith(run_id):
                    break

def read_system_config(args, dockerconf):
    if args.systemconfig:
        f = args.systemconfig
    else:
        f = os.path.join(args.datadir, "galaxy", "bcbio_system.yaml")
    with open(f) as in_handle:
        config = yaml.load(in_handle)
    # Map external galaxy specifications over to docker container
    dmounts = []
    for k in ["galaxy_config"]:
        if k in config:
            dirname, base = os.path.split(os.path.normpath(os.path.realpath(config[k])))
            container_dir = os.path.join(dockerconf["input_dir"], "system", "galaxy", k)
            dmounts.append("%s:%s" % (dirname, container_dir))
            dmounts.extend(mounts.find_genome_directory(dirname, container_dir))
            config[k] = os.path.join(container_dir, base)
    return config, dmounts
