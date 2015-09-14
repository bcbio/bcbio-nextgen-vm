"""
Shared constants across the bcbio-nextgen-vm project.
"""

import logging
import os
import sys

# pylint: disable=no-init,old-style-class

NFS_OPTIONS = "rw,async,nfsvers=3"  # NFS tuning
DEFAULT_PERMISSIONS = 0o644
DEFAULT_PROVIDER = 'aws'


DOCKER = {
    "port": 8085,
    "biodata_dir": "/usr/local/share/bcbio-nextgen",
    "work_dir": "/mnt/work",
    "image_url": ("https://s3.amazonaws.com/bcbio_nextgen/"
                  "bcbio-nextgen-docker-image.gz")
}
DOCKER_DEFAULT_IMAGE = "chapmanb/bcbio-nextgen-devel"


class ANSIBLE:

    """Ansible specific settings."""

    FORKS = 10
    KEY_CHECKING = "False"


class PATH:

    """Default paths used across the project."""

    ANSIBLE_BASE = os.path.join(sys.prefix, "share", "bcbio-vm", "ansible")
    BCBIO = os.path.join(os.path.expanduser("~"), '.bcbio')
    EC = os.path.join(BCBIO, "elasticluster")
    EC_ANSIBLE_LIBRARY = os.path.join(sys.prefix, "share", "elasticluster",
                                      "providers", "ansible-playbooks",
                                      "library")
    EC_STORAGE = os.path.join(EC, "storage")
    EC_CONFIG = os.path.join(EC, "{provider}.config")
    PICKLE_FILE = os.path.join(EC_STORAGE, "%(cluster)s.pickle")

    ANSIBLE_TEMPLATE = os.path.join(ANSIBLE_BASE, "ansible-icel.cfg.template")
    EC_CONFIG_TEMPLATE = os.path.join(sys.prefix, "share", "bcbio-vm",
                                      "elasticluster", "{provider}.config")
    SSH_TEMPLATE = os.path.join(ANSIBLE_BASE, "ssh_config-icel.template")


class PROVIDER:

    """Contains the available providers' name."""

    AWS = "aws"
    AZURE = "azure"


class SSH:

    """SSH specific settings."""

    HOST = '127.0.0.1'
    PORT = 22
    USER = 'root'
    PROXY = ('ssh -o VisualHostKey=no -W %(host)s:%(port)d '
             '%(user)s@%(bastion)s')


ENVIRONMENT = {
    "development": {
        "log.name": "bcbiovm-devel",
        "log.cli_level": logging.DEBUG,
        "log.file_level": logging.DEBUG,
        "log.format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "log.file": None,
        "misc.attempts": 3,
        "misc.retry_interval": 0.1,
    },
    "production": {
        "log.name": "bcbiovm",
        "log.cli_level": logging.INFO,
        "log.file_level": logging.INFO,
        "log.format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "log.file": None,
        "misc.attempts": 5,
        "misc.retry_interval": 0.2,
    }
}
