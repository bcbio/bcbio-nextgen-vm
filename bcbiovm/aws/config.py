"""Handle remote configuration inputs specified via S3 buckets.
"""
import os

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
    out_file = os.path.join(utils.safe_makedir(os.path.join(os.getcwd(), "config")),
                            os.path.basename(key))
    with open(out_file, "w") as out_handle:
        yaml.dump(config, out_handle, default_flow_style=False, allow_unicode=False)
    return out_file
