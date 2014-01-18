"""Save and retrieve default locations associated with a bcbio-nextgen installation.
"""
from __future__ import print_function
from __future__ import unicode_literals
from future.builtins import open
import os
import sys

import yaml

TOSAVE_DEFAULTS = {"datadir": None}

def update_check_args(args, command_info):
    args = add_defaults(args)
    if not args.datadir:
        print("Must specify a `--datadir` or save the default location with `saveconfig`.\n" + command_info)
        sys.exit(1)
    return args

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
