"""
Save and retrieve default locations associated with a
bcbio-nextgen installation.
"""

from __future__ import print_function
import os
import sys

import yaml
from bcbio.distributed import objectstore
from bcbio import utils

from bcbiovm.common import constant

TOSAVE_DEFAULTS = {"datadir": None}


def load_s3(sample_config):
    """Move a sample configuration locally, providing remote upload."""
    with objectstore.open(sample_config) as in_handle:
        config = yaml.load(in_handle)
    r_sample_config = objectstore.parse_remote(sample_config)
    config["upload"] = {
        "method": "s3",
        "dir": os.path.join(os.pardir, "final"),
        "bucket": r_sample_config.bucket,
        "folder": os.path.join(os.path.dirname(r_sample_config.key), "final")
    }
    region = (r_sample_config.region or
              objectstore.default_region(sample_config))
    if region:
        config["upload"]["region"] = region

    if not os.access(os.pardir, os.W_OK | os.X_OK):
        raise IOError(
            "Cannot write to the parent directory of work directory %s\n"
            "bcbio wants to store prepared uploaded files to %s\n"
            "We recommend structuring your project in a project specific "
            "directory structure\n"
            "with a specific work directory (mkdir -p your-project/work "
            "&& cd your-project/work)." %
            (os.getcwd(), os.path.join(os.pardir, "final")))
    config = _add_jar_resources(config, sample_config)
    out_file = os.path.join(utils.safe_makedir(os.path.join(os.getcwd(),
                                                            "config")),
                            os.path.basename(r_sample_config.key))
    with open(out_file, "w") as out_handle:
        yaml.dump(config, out_handle, default_flow_style=False,
                  allow_unicode=False)
    return out_file


def _add_jar_resources(config, sample_config):
    """Find uploaded jars for GATK and MuTect relative to input file.

    Automatically puts these into the configuration file to make them available
    for downstream processing. Searches for them in the specific project folder
    and also a global jar directory for a bucket.
    """
    base, rest = sample_config.split("//", 1)
    for dirname in (os.path.join("%s//%s" % (base, rest.split("/")[0]),
                                 "jars"),
                    os.path.join(os.path.dirname(sample_config), "jars")):
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
                config["resources"][prog]["jar"] = str(fname)
    return config


def update_check_args(args, command_info, need_datadir=True):
    args = add_defaults(args)
    args = _handle_remotes(args)
    if not args.datadir:
        default_datadir = _find_default_datadir(need_datadir)
        if default_datadir:
            args.datadir = default_datadir
        else:
            print("Must specify a `--datadir` or save the default "
                  "location with `saveconfig`.\n" + command_info)
            sys.exit(1)
    return args


def _handle_remotes(args):
    """Retrieve supported remote inputs specified on the command line.
    """
    if hasattr(args, "sample_config"):
        if objectstore.is_remote(args.sample_config):
            if args.sample_config.startswith("s3://"):
                args.sample_config = load_s3(args.sample_config)
            else:
                raise NotImplementedError("Do not recognize remote input %s" %
                                          args.sample_config)
    return args


def _find_default_datadir(must_exist=True):
    """Check if the default data directory/standard setup is present."""
    if (os.path.exists(constant.PATH.INSTALL_PARAMS) and
            os.path.exists(constant.PATH.BCBIO_SYSTEM)):
        return constant.PATH.DATADIR

    elif not must_exist:
        return constant.PATH.DATADIR

    else:
        return None


def save(args):
    """Save user specific defaults to a yaml configuration file."""
    out = get_defaults()
    for k in TOSAVE_DEFAULTS:
        karg = getattr(args, k, None)
        if karg and karg != TOSAVE_DEFAULTS[k]:
            out[k] = karg
    if len(out) > 0:
        with open(_get_config_file(just_filename=True), "w") as out_handle:
            yaml.dump(out, out_handle, default_flow_style=False,
                      allow_unicode=False)


def add_defaults(args):
    """Add user configured defaults to supplied command line arguments."""
    config_defaults = get_defaults()
    for k in TOSAVE_DEFAULTS:
        karg = getattr(args, k, None)
        if not karg or karg == TOSAVE_DEFAULTS[k]:
            if k in config_defaults:
                setattr(args, k, config_defaults[k])
    return args


def get_datadir():
    """Retrieve the default data directory for this installation
    """
    datadir = get_defaults().get("datadir")
    if datadir is None:
        datadir = _find_default_datadir()
    return datadir


def get_defaults():
    """Retrieve saved default configurations.
    """
    config_file = _get_config_file()
    if config_file:
        with open(config_file) as in_handle:
            return yaml.load(in_handle)
    else:
        return {}


def _get_config_file(just_filename=False):
    """Retrieve standard user configuration file.
    Uses location from appdirs (https://github.com/ActiveState/appdirs). Could
    pull this in as dependency for more broad platform support.
    """
    config_dir = os.path.join(os.getenv('XDG_CONFIG_HOME',
                                        os.path.expanduser("~/.config")),
                              "bcbio-nextgen")
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
    config_file = os.path.join(config_dir, "bcbio-docker-config.yaml")
    if just_filename or os.path.exists(config_file):
        return config_file
    else:
        return None
