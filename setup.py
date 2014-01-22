#!/usr/bin/env python
import os
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

# conda build
if "--record=/dev/null" in sys.argv:
    install_requires = []
else:
    install_requires = ["six", "requests>=2.1.0", "PyYAML", "progressbar"]

setup(name="bcbio-nextgen-vm",
      version=version,
      author="Brad Chapman and bcbio-nextgen contributors",
      description="Run bcbio-nextgen genomic sequencing pipelines using isolated containers and virtual machines",
      license="MIT",
      url="https://github.com/chapmanb/bcbio-nextgen-vm",
      packages=find_packages(),
      scripts=["scripts/bcbio_nextgen_docker.py"],
      install_requires=install_requires)
