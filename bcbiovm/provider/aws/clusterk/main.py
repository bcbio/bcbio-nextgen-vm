"""Main entry point to run distributed clusterk analyses.
"""
import os

import yaml

from bcbiovm.docker import manage, mounts


def prep_s3(biodata_bucket, run_bucket, output_folder):
    """Prepare configuration for shipping to S3."""
    return {
        "type": "S3",
        "buckets": {
            "run": run_bucket,
            "biodata": biodata_bucket,
        },
        "folders": {
            "output": output_folder,
        }
    }


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
    parallel["pack"] = prep_s3(args.biodata_bucket, args.run_bucket,
                               "runfn_output")
    parallel["wrapper_args"] = [{"sample_config": ready_config_file,
                                 "docker_config": docker_config,
                                 "fcdir": args.fcdir,
                                 "datadir": args.datadir,
                                 "systemconfig": args.systemconfig}]
    workdir_mount = "%s:%s" % (work_dir, docker_config["work_dir"])
    manage.run_bcbio_cmd(args.image, [workdir_mount],
                         ["version",
                          "--workdir=%s" % docker_config["work_dir"]])

    from bcbio.pipeline import main
    main.run_main(work_dir, run_info_yaml=ready_config_file,
                  config_file=args.systemconfig, fc_dir=args.fcdir,
                  parallel=parallel)
