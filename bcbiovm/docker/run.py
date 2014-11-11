"""Run a bcbio-nextgen analysis inside of an isolated docker container.
"""
from __future__ import print_function

import os
import uuid
import sys

import yaml

from bcbio import log
from bcbiovm.docker import manage, mounts, remap
from bcbiovm.ship import reconstitute

def do_analysis(args, dockerconf):
    """Run a full analysis on a local machine, utilizing multiple cores.
    """
    work_dir = os.getcwd()
    with open(args.sample_config) as in_handle:
        sample_config, dmounts = mounts.update_config(yaml.load(in_handle), dockerconf["input_dir"], args.fcdir)
    dmounts += mounts.prepare_system(args.datadir, dockerconf["biodata_dir"])
    dmounts.append("%s:%s" % (work_dir, dockerconf["work_dir"]))
    system_config, system_mounts = _read_system_config(dockerconf, args.systemconfig, args.datadir)
    system_cfile = os.path.join(work_dir, "bcbio_system-forvm.yaml")
    sample_cfile = os.path.join(work_dir, "bcbio_sample-forvm.yaml")
    with open(system_cfile, "w") as out_handle:
        yaml.dump(system_config, out_handle, default_flow_style=False, allow_unicode=False)
    with open(sample_cfile, "w") as out_handle:
        yaml.dump(sample_config, out_handle, default_flow_style=False, allow_unicode=False)
    in_files = [os.path.join(dockerconf["work_dir"], os.path.basename(x)) for x in [system_cfile, sample_cfile]]
    log.setup_local_logging({"include_time": False})
    manage.run_bcbio_cmd(args.image, dmounts + system_mounts,
                         in_files + ["--numcores", str(args.numcores), "--workdir=%s" % dockerconf["work_dir"]])

def do_runfn(fn_name, fn_args, cmd_args, parallel, dockerconf, ports=None):
    """"Run a single defined function inside a docker container, returning results.
    """
    dmounts = []
    if cmd_args.get("sample_config"):
        with open(cmd_args["sample_config"]) as in_handle:
            _, dmounts = mounts.update_config(yaml.load(in_handle), None, cmd_args["fcdir"])
    datadir, fn_args = reconstitute.prep_datadir(cmd_args["pack"], fn_args)
    if "orig_systemconfig" in cmd_args:
        orig_sconfig = _get_system_configfile(cmd_args["orig_systemconfig"], datadir)
        orig_galaxydir = os.path.dirname(orig_sconfig)
        dmounts.append("%s:%s" % (orig_galaxydir, orig_galaxydir))
    work_dir, fn_args, finalizer = reconstitute.prep_workdir(cmd_args["pack"], parallel, fn_args)
    dmounts += mounts.prepare_system(datadir, dockerconf["biodata_dir"])
    reconstitute.prep_systemconfig(datadir, fn_args)
    _, system_mounts = _read_system_config(dockerconf, cmd_args["systemconfig"], datadir)

    dmounts.append("%s:%s" % (work_dir, dockerconf["work_dir"]))
    all_mounts = dmounts + system_mounts

    argfile = os.path.join(work_dir, "runfn-%s-%s.yaml" % (fn_name, uuid.uuid4()))
    with open(argfile, "w") as out_handle:
        yaml.safe_dump(remap.external_to_docker(fn_args, all_mounts),
                       out_handle, default_flow_style=False, allow_unicode=False)
    docker_argfile = os.path.join(dockerconf["work_dir"], os.path.basename(argfile))
    outfile = "%s-out%s" % os.path.splitext(argfile)
    out = None
    manage.run_bcbio_cmd(cmd_args["image"], all_mounts,
                         ["runfn", fn_name, docker_argfile],
                         ports=ports)
    if os.path.exists(outfile):
        with open(outfile) as in_handle:
            out = remap.docker_to_external(yaml.safe_load(in_handle), all_mounts)
    else:
        print("Subprocess in docker container failed")
        sys.exit(1)
    out = finalizer(out)
    for f in [argfile, outfile]:
        if os.path.exists(f):
            os.remove(f)
    return out

def local_system_config(systemconfig, datadir, work_dir):
    """Create a ready to run local system configuration file.
    """
    config = _get_system_config(systemconfig, datadir)
    system_cfile = os.path.join(work_dir, "bcbio_system-prep.yaml")
    with open(system_cfile, "w") as out_handle:
        yaml.dump(config, out_handle, default_flow_style=False, allow_unicode=False)
    return system_cfile

def _get_system_configfile(systemconfig, datadir):
    """Retrieve system configuration file from input or default directory.
    """
    if systemconfig:
        if not os.path.isabs(systemconfig):
            return os.path.normpath(os.path.join(os.getcwd(), systemconfig))
        else:
            return systemconfig
    else:
        return os.path.join(datadir, "galaxy", "bcbio_system.yaml")

def _get_system_config(systemconfig, datadir):
    """Retrieve a system configuration with galaxy references specified.
    """
    f = _get_system_configfile(systemconfig, datadir)
    with open(f) as in_handle:
        config = yaml.load(in_handle)
    if "galaxy_config" not in config:
        config["galaxy_config"] = os.path.join(os.path.dirname(f), "universe_wsgi.ini")
    return config

def _read_system_config(dockerconf, systemconfig, datadir):
    config = _get_system_config(systemconfig, datadir)
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
