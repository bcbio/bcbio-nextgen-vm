"""
Shared constants across the bcbio-nextgen-vm project.
"""
import os
import sys

NFS_OPTIONS = "rw,async,nfsvers=3"  # NFS tuning
DEFAULT_PERMISSIONS = 0o644
DEFAULT_PROVIDER = 'aws'

AWS_ICEL_TEMPLATES = {
    'ap-northeast-1': 'http://s3-ap-northeast-1.amazonaws.com/'
                      'hpdd-templates-ap-northeast-1/gs/1.0.1/'
                      'hpdd-gs-ha-c3-small-1.0.1.template',
    'ap-southeast-1': 'http://s3-ap-southeast-1.amazonaws.com/'
                      'hpdd-templates-ap-southeast-1/gs/1.0.1/'
                      'hpdd-gs-ha-c3-small-1.0.1.template',
    'ap-southeast-2': 'http://s3-ap-southeast-2.amazonaws.com/'
                      'hpdd-templates-ap-southeast-2/gs/1.0.1/'
                      'hpdd-gs-ha-c3-small-1.0.1.template',
    'eu-west-1': 'http://s3-eu-west-1.amazonaws.com/hpdd-templates-eu-west-1'
                 '/gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template',
    'sa-east-1': 'http://s3-sa-east-1.amazonaws.com/hpdd-templates-sa-east-1'
                 '/gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template',
    'us-east-1': 'http://s3.amazonaws.com/hpdd-templates-us-east-1/gs/1.0.1/'
                 'hpdd-gs-ha-c3-small-1.0.1.template',
    'us-west-1': 'http://s3-us-west-1.amazonaws.com/hpdd-templates-us-west-1'
                 '/gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template',
    'us-west-2': 'http://s3-us-west-2.amazonaws.com/hpdd-templates-us-west-2'
                 '/gs/1.0.1/hpdd-gs-ha-c3-small-1.0.1.template',
}

DOCKER = {
    "port": 8085,
    "biodata_dir": "/usr/local/share/bcbio-nextgen",
    "work_dir": "/mnt/work",
    "image_url": ("https://s3.amazonaws.com/bcbio_nextgen/"
                  "bcbio-nextgen-docker-image.gz")
}

IAM_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "*",
      "Resource": "*"
    }
  ]
}
"""

S3_POLICY = """{
    "Version": "2012-10-17",
    "Statement": [
        {
              "Effect": "Allow",
              "Action": "s3:*",
              "Resource": "*"
            }
      ]
}
"""


class ANSIBLE:

    """Ansible specific settings."""

    FORKS = 10
    KEY_CHECKING = "False"


class MISC:

    """Miscellaneous settings."""

    ATTEMPTS = 3
    RETRY_INTERVAL = 0.1


class PATH:

    """Default paths used across the project."""

    ANSIBLE_BASE = os.path.join(sys.prefix, "share", "bcbio-vm", "ansible")
    BCBIO = os.path.join(os.path.expanduser("~"), '.bcbio')
    DATADIR = os.path.realpath(os.path.normpath(
        os.path.join(os.path.dirname(sys.executable), os.pardir, os.pardir,
                     "data")))
    EC = os.path.join(BCBIO, "elasticluster")
    EC_ANSIBLE_LIBRARY = os.path.join(sys.prefix, "share", "elasticluster",
                                      "providers", "ansible-playbooks",
                                      "library")
    EC_CONFIG = os.path.join(EC, "config")
    EC_STORAGE = os.path.join(EC, "storage")
    PICKLE_FILE = os.path.join(EC_STORAGE, "%(cluster)s.pickle")

    ANSIBLE_TEMPLATE = os.path.join(ANSIBLE_BASE, "ansible-icel.cfg.template")
    EC_CONFIG_TEMPLATE = os.path.join(sys.prefix, "share", "bcbio-vm",
                                      "elasticluster", "config")
    SSH_TEMPLATE = os.path.join(ANSIBLE_BASE, "ssh_config-icel.template")

    INSTALL_PARAMS = os.path.join(DATADIR, "config", "install-params.yaml")
    BCBIO_SYSTEM = os.path.join(DATADIR, "galaxy", "bcbio_system.yaml")


class PLAYBOOK:

    """Default paths for Ansible playbooks."""

    BCBIO = os.path.join(PATH.ANSIBLE_BASE, "roles", "bcbio_bootstrap",
                         "tasks", "main.yml")
    DOCKER = os.path.join(PATH.ANSIBLE_BASE, "roles", "docker", "tasks",
                          "main.yml")
    DOCKER_LOCAl = os.path.join(PATH.ANSIBLE_BASE, "bcbio_vm_docker_local.yml")
    GOF3R = os.path.join(PATH.ANSIBLE_BASE, "roles", "gof3r", "tasks",
                         "main.yml")
    NFS = os.path.join(PATH.ANSIBLE_BASE, "roles", "encrypted_nfs", "tasks",
                       "main.yml")
    ICEL = os.path.join(PATH.ANSIBLE_BASE, "roles", "icel", "tasks",
                        "main.yml")
    MOUNT_LUSTRE = os.path.join(PATH.ANSIBLE_BASE, "roles", "lustre_client",
                                "tasks", "mount.yml")
    UNMOUNT_LUSTRE = os.path.join(PATH.ANSIBLE_BASE, "roles", "lustre_client",
                                  "tasks", "unmount.yml")


class SSH:

    """SSH specific settings."""

    HOST = '127.0.0.1'
    PORT = 22
    USER = 'root'
    PROXY = ('ssh -o VisualHostKey=no -W %(host)s:%(port)d '
             '%(user)s@%(bastion)s')


class LOG:

    """Logging default values."""

    NAME = "bcbiovm"
    LEVEL = 10
    FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    FILE = ""
