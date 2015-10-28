"""Default paths for Ansible playbooks."""
import os
import sys


class Playbook(object):

    """Default paths for Ansible playbooks."""

    base = (sys.prefix, "share", "bcbio-vm", "ansible")
    bcbio = ("roles", "bcbio_bootstrap", "tasks", "main.yml")
    docker = ("roles", "docker", "tasks", "main.yml")
    docker_local = ("bcbio_vm_docker_local.yml", )
    gof3r = ("roles", "gof3r", "tasks", "main.yml")
    nfs = ("roles", "encrypted_nfs", "tasks", "main.yml")

    def __getattribute__(self, name):
        """Get the path for the received playbook."""
        attribute = super(Playbook, self).__getattribute__(name)
        if isinstance(attribute, tuple):
            attribute = os.path.join(*attribute)
            if name != "base":
                base = getattr(self, "base")
                attribute = os.path.join(base, attribute)
            setattr(self, name, attribute)

        return attribute
