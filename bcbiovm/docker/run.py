"""Run a bcbio-nextgen analysis inside of an isolated docker container.
"""
from __future__ import print_function

import os

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
    system_cfile = os.path.join(os.getcwd(), "bcbio_system-forvm.yaml")
    sample_cfile = os.path.join(os.getcwd(), "bcbio_sample-forvm.yaml")
    with open(system_cfile, "w") as out_handle:
        yaml.dump(system_config, out_handle, default_flow_style=False, allow_unicode=False)
    with open(sample_cfile, "w") as out_handle:
        yaml.dump(sample_config, out_handle, default_flow_style=False, allow_unicode=False)
    in_files = [os.path.join(dockerconf["work_dir"], os.path.basename(x)) for x in [system_cfile, sample_cfile]]
    manage.run_bcbio_cmd(dockerconf["image"], dmounts + system_mounts,
                         "{} --workdir={}".format(" ".join(in_files), dockerconf["work_dir"]))

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
            config[k] = str(os.path.join(container_dir, base))
    return config, dmounts
