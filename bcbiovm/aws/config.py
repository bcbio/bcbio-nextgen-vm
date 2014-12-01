"""Handle remote configuration inputs specified via S3 buckets.
"""
import os

import boto
import yaml

from bcbio import utils

def load_s3(sample_config):
    """Move a sample configuration locally, providing remote upload.
    """
    with utils.s3_handle(sample_config) as in_handle:
        config = yaml.load(in_handle)
    bucket, key = utils.s3_bucket_key(sample_config)
    config["upload"] = {"method": "s3",
                        "dir": os.path.join(os.pardir, "final"),
                        "bucket": bucket,
                        "folder": os.path.join(os.path.dirname(key), "final")}
    if not os.access(os.pardir, os.W_OK | os.X_OK):
        raise IOError("Cannot write to the parent directory of work directory %s\n"
                      "bcbio wants to store prepared uploaded files to %s\n"
                      "We recommend structuring your project in a project specific directory structure\n"
                      "with a specific work directory (mkdir -p your-project/work && cd your-project/work)."
                      % (os.getcwd(), os.path.join(os.pardir, "final")))
    config = _add_jar_resources(config, bucket, key)
    out_file = os.path.join(utils.safe_makedir(os.path.join(os.getcwd(), "config")),
                            os.path.basename(key))
    with open(out_file, "w") as out_handle:
        yaml.dump(config, out_handle, default_flow_style=False, allow_unicode=False)
    return out_file

def _add_jar_resources(config, bucket_name, key_name):
    """Find uploaded jars for GATK and MuTect relative to input file.

    Automatically puts these into the configuration file to make them available
    for downstream processing.
    """
    conn = boto.connect_s3()
    bucket = conn.get_bucket(bucket_name)
    prefix = os.path.join(os.path.dirname(key_name), "jars")
    for key in bucket.get_all_keys(prefix=prefix):
        if key.name.lower().find("genomeanalysistk") >= 0:
            prog = "gatk"
        elif key.name.lower().find("mutect") >= 0:
            prog = "mutect"
        else:
            prog = None
        if prog:
            if "resources" not in config:
                config["resources"] = {}
            if prog not in config["resources"]:
                config["resources"][prog] = {}
            config["resources"][prog]["jar"] = str("s3://%s/%s" % (bucket.name, key.name))
    return config
