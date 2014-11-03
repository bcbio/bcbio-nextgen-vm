"""Save and retrieve default locations associated with a bcbio-nextgen installation.
"""
from __future__ import print_function
import os
import sys

import yaml

from bcbio import utils
from bcbiovm.aws import config as awsconfig

TOSAVE_DEFAULTS = {"datadir": None}

def update_check_args(args, command_info, need_datadir=True):
    args = add_defaults(args)
    args = _handle_remotes(args)
    if not args.datadir:
        default_datadir = _find_default_datadir(need_datadir)
        if default_datadir:
            args.datadir = default_datadir
        else:
            print("Must specify a `--datadir` or save the default location with `saveconfig`.\n" + command_info)
            sys.exit(1)
    return args

def _handle_remotes(args):
    """Retrieve supported remote inputs specified on the command line.
    """
    if args.sample_config.startswith(utils.SUPPORTED_REMOTES):
        if args.sample_config.startswith("s3://"):
            args.sample_config = awsconfig.load_s3(args.sample_config)
        else:
            raise NotImplementedError("Do not recognize remote input %s" % args.sample_config)
    return args

def _find_default_datadir(must_exist=True):
    """Check if the default data directory/standard setup is present
    """
    datadir = os.path.realpath(os.path.normpath(os.path.join(
        os.path.dirname(sys.executable), os.pardir, os.pardir, "data")))
    if (os.path.exists(os.path.join(datadir, "config", "install-params.yaml")) and
          os.path.exists(os.path.join(datadir, "galaxy", "bcbio_system.yaml"))):
        return datadir
    elif not must_exist:
        return datadir
    else:
        return None

def save(args):
    """Save user specific defaults to a yaml configuration file.
    """
    out = get_defaults()
    for k in TOSAVE_DEFAULTS:
        karg = getattr(args, k, None)
        if karg and karg != TOSAVE_DEFAULTS[k]:
            out[k] = karg
    if len(out) > 0:
        with open(_get_config_file(just_filename=True), "w") as out_handle:
            yaml.dump(out, out_handle, default_flow_style=False, allow_unicode=False)

def add_defaults(args):
    """Add user configured defaults to supplied command line arguments.
    """
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
    config_dir = os.path.join(os.getenv('XDG_CONFIG_HOME', os.path.expanduser("~/.config")),
                              "bcbio-nextgen")
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
    config_file = os.path.join(config_dir, "bcbio-docker-config.yaml")
    if just_filename or os.path.exists(config_file):
        return config_file
    else:
        return None
