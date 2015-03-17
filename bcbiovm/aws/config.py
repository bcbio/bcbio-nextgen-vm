"""Handle remote configuration inputs specified via S3 buckets.
"""
import os

import yaml

from bcbio.distributed import objectstore
from bcbio import utils

def load_s3(sample_config):
    """Move a sample configuration locally, providing remote upload.
    """
    with objectstore.open(sample_config) as in_handle:
        config = yaml.load(in_handle)
    r_sample_config = objectstore.parse_remote(sample_config)
    config["upload"] = {"method": "s3",
                        "dir": os.path.join(os.pardir, "final"),
                        "bucket": r_sample_config.bucket,
                        "folder": os.path.join(os.path.dirname(r_sample_config.key), "final")}
    region = r_sample_config.region or objectstore.default_region(sample_config)
    if region:
        config["upload"]["region"] = region
    if not os.access(os.pardir, os.W_OK | os.X_OK):
        raise IOError("Cannot write to the parent directory of work directory %s\n"
                      "bcbio wants to store prepared uploaded files to %s\n"
                      "We recommend structuring your project in a project specific directory structure\n"
                      "with a specific work directory (mkdir -p your-project/work && cd your-project/work)."
                      % (os.getcwd(), os.path.join(os.pardir, "final")))
    config = _add_jar_resources(config, sample_config)
    out_file = os.path.join(utils.safe_makedir(os.path.join(os.getcwd(), "config")),
                            os.path.basename(r_sample_config.key))
    with open(out_file, "w") as out_handle:
        yaml.dump(config, out_handle, default_flow_style=False, allow_unicode=False)
    return out_file

def _add_jar_resources(config, sample_config):
    """Find uploaded jars for GATK and MuTect relative to input file.

    Automatically puts these into the configuration file to make them available
    for downstream processing. Searches for them in the specific project folder
    and also a global jar directory for a bucket.
    """
    base, rest = config.split("//", 1)
    for dirname in [os.path.join("%s//%s" % (base, rest.split("/")[0]), "jars"),
                    os.path.join(os.path.dirname(sample_config), "jars")]:
        for fname in objectstore.list(dirname):
            if fname.lower().find("genomeanalysistk") >= 0:
                prog = "gatk"
            elif fname.lower().find("mutect") >= 0:
                prog = "mutect"
            else:
                prog = None
            if prog:
                if "resources" not in config:
                    config["resources"] = {}
                if prog not in config["resources"]:
                    config["resources"][prog] = {}
                config["resources"][prog]["jar"] = fname
    return config
