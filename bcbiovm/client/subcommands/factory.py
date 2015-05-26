"""Sub-commands factory."""
from bcbiovm.client.subcommands import cluster
from bcbiovm.client.subcommands import config
from bcbiovm.client.subcommands import docker
from bcbiovm.client.subcommands import icel
from bcbiovm.client.subcommands import ipython

# TODO(alexandrucoman): Add support for dynamically loading subcommands

_SUBCOMMANDS = {
    'cluster': {
        'Bootstrap': cluster.Bootstrap,
        'Command': cluster.Command,
        'Setup': cluster.Setup,
        'Start': cluster.Start,
        'Stop': cluster.Stop,
        'SSHConnection': cluster.SSHConnection,
    },
    'config': {
        'Edit': config.Edit,
    },
    'docker': {
        'BiodataUpload': docker.BiodataUpload,
        'Build': docker.Build,
        'Install': docker.Install,
        'Server': docker.Server,
        'SetupInstall': docker.SetupInstall,
        'SystemUpdate': docker.SystemUpdate,
        'Run': docker.Run,
        'RunFunction': docker.RunFunction,
        'Upgrade': docker.Upgrade,
    },
    'icel': {
        'Create': icel.Create,
        'Mount': icel.Mount,
        'Unmount': icel.Unmount,
        'Stop': icel.Stop,
        'Specification': icel.Specification
    },
    'ipython': {
        'IPython': ipython.IPython,
        'IPythonPrep': ipython.IPythonPrep,
    }
}


def get(container, name):
    """Return the required subcommand."""
    # TODO(alexandrucoman): Check the received information
    return _SUBCOMMANDS[container][name]
