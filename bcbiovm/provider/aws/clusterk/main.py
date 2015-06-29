"""Main entry point to run distributed clusterk analyses.
"""
import os

import yaml
from bcbio.pipeline import main

from bcbiovm.docker import manage, mounts
from bcbiovm.provider import factory as provider_factory


def run(args, docker_config):
    work_dir = os.getcwd()
    parallel = {
        "type": "clusterk",
        "queue": args.queue,
        "cores": args.numcores,
        "module": "bcbiovm.provider.aws.clusterk",
        "wrapper": "runfn"
    }

    with open(args.sample_config) as in_handle:
        ready_config, _ = mounts.normalize_config(yaml.load(in_handle),
                                                  args.fcdir)

    ready_config_file = os.path.splitext(os.path.basename(args.sample_config))
    ready_config_file = os.path.join(work_dir,
                                     "%s-ready%s" % ready_config_file)

    with open(ready_config_file, "w") as out_handle:
        yaml.safe_dump(ready_config, out_handle,
                       default_flow_style=False,
                       allow_unicode=False)

    ship_conf = provider_factory.get_ship_config("S3")
    parallel["pack"] = ship_conf(args.biodata_bucket, args.run_bucket,
                                 "runfn_output").to_dict()

    parallel["wrapper_args"] = [{"sample_config": ready_config_file,
                                 "docker_config": docker_config,
                                 "fcdir": args.fcdir,
                                 "datadir": args.datadir,
                                 "systemconfig": args.systemconfig}]
    workdir_mount = "%s:%s" % (work_dir, docker_config["work_dir"])
    manage.run_bcbio_cmd(args.image, [workdir_mount],
                         ["version",
                          "--workdir=%s" % docker_config["work_dir"]])

    main.run_main(work_dir, run_info_yaml=ready_config_file,
                  config_file=args.systemconfig, fc_dir=args.fcdir,
                  parallel=parallel)
