#!/usr/bin/env python
import os
import shutil
import sys
from setuptools import setup, find_packages

version = "0.1.0a"

def write_version_py():
    version_py = os.path.join(os.path.dirname(__file__), "bcbiovm", "version.py")
    try:
        import subprocess
        p = subprocess.Popen(["git", "rev-parse", "--short", "HEAD"],
                             stdout=subprocess.PIPE)
        githash = p.stdout.read().strip()
    except:
        githash = ""
    with open(version_py, "w") as out_handle:
        out_handle.write("\n".join(['__version__ = "%s"' % version,
                                    '__git_revision__ = "%s"' % githash]))

write_version_py()

if "--record=/dev/null" in sys.argv:  # conda build
    install_requires = []
else:
    install_requires = [
        "matplotlib", "pandas", "paramiko", "pylab", "six", "PyYAML",
        "bcbio-nextgen"]

setup(name="bcbio-nextgen-vm",
      version=version,
      author="Brad Chapman and bcbio-nextgen contributors",
      description="Run bcbio-nextgen genomic sequencing analyses using isolated containers and virtual machines",
      license="MIT",
      url="https://github.com/chapmanb/bcbio-nextgen-vm",
      packages=find_packages(),
      scripts=["scripts/bcbio_vm.py"],
      install_requires=install_requires)

def ansible_pb_files(ansible_pb_dir):
    """Retrieve ansible files for installation. Derived from elasticluster setup.
    """
    ansible_data = []
    for (dirname, dirnames, filenames) in os.walk(ansible_pb_dir):
        tmp = []
        for fname in filenames:
            if fname.startswith(".git"): continue
            tmp.append(os.path.join(dirname, fname))
        ansible_data.append((os.path.join("share", "bcbio-vm", dirname), tmp))
    return ansible_data

def elasticluster_config_files(base_dir):
    """Retrieve example elasticluster config files for installation.
    """
    return [(os.path.join("share", "bcbio-vm", base_dir),
             [os.path.join(base_dir, x) for x in os.listdir(base_dir)])]

if __name__ == "__main__":
    """Install ansible playbooks and other associated data files.
    """
    if sys.argv[1] in ["develop", "install"]:
        for dirname, fnames in ansible_pb_files("ansible") + elasticluster_config_files("elasticluster"):
            dirname = os.path.join(os.path.abspath(sys.prefix), dirname)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            for fname in fnames:
                if sys.argv[1] == "develop":
                    link_path = os.path.join(dirname, os.path.basename(fname))
                    if not os.path.exists(link_path):
                        link_target = os.path.join(os.getcwd(), fname)
                        os.symlink(link_target, link_path)
                else:
                    shutil.copy(fname, dirname)
