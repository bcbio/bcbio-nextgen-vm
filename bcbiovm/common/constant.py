"""
Shared constants across the bcbio-nextgen-vm project.
"""
import os
import sys

NFS_OPTIONS = "rw,async,nfsvers=3"  # NFS tuning
DEFAULT_PERMISSIONS = 0o644


class ANSIBLE:

    FORKS = 10
    KEY_CHECKING = "False"


class MISC:

    ATTEMPTS = 3
    RETRY_INTERVAL = 0.1


class PATH:

    ANSIBLE_BASE = os.path.join(sys.prefix, "share", "bcbio-vm", "ansible")
    BCBIO = os.path.join(os.path.expanduser("~"), '.bcbio')
    EC = os.path.join(BCBIO, "elasticluster")
    EC_ANSIBLE_LIBRARY = os.path.join(sys.prefix, "share", "elasticluster",
                                      "providers", "ansible-playbooks",
                                      "library")
    EC_CONFIG = os.path.join(EC, "config")
    EC_STORAGE = os.path.join(EC, "storage")
    PICKLE_FILE = os.path.join(EC_STORAGE, "%(cluster)s.pickle")


class PLAYBOOK:

    BCBIO = os.path.join(PATH.ANSIBLE_BASE, "roles", "bcbio_bootstrap",
                         "tasks", "main.yml")
    DOCKER = os.path.join(PATH.ANSIBLE_BASE, "roles", "docker", "tasks",
                          "main.yml")
    GOF3R = os.path.join(PATH.ANSIBLE_BASE, "roles", "gof3r", "tasks",
                         "main.yml")
    NFS = os.path.join(PATH.ANSIBLE_BASE, "roles", "encrypted_nfs", "tasks",
                       "main.yml")


class SSH:

    HOST = '127.0.0.1'
    PORT = 22
    USER = 'root'
    PROXY = ('ssh -o VisualHostKey=no -W %(host)s:%(port)d '
             '%(user)s@%(bastion)s')


class LOG:

    NAME = "bcbiovm"
    LEVEL = 10
    FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    FILE = ""
