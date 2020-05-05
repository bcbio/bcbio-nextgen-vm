import os
import shutil
import subprocess

import bcbio.utils

from tests.conftest import install_cwl_test_files


def test_cwl_docker_somatic_workflow():
    with install_cwl_test_files() as workdir:
        with bcbio.utils.chdir(os.path.join(workdir, "somatic")):
            subprocess.check_call(["bash", "./run_generate_cwl.sh"])
            if os.path.exists("cromwell_work"):
                shutil.rmtree("cromwell_work")
            subprocess.check_call(["bcbio_vm.py", "cwlrun", "cromwell", "somatic-workflow"])


def test_cwl_rnaseq(install_test_files):
    with install_cwl_test_files() as work_dir:
        with bcbio.utils.chdir(os.path.join(work_dir, "rnaseq")):
            if os.path.exists("cromwell_work"):
                shutil.rmtree("cromwell_work")
            subprocess.check_call(["bcbio_vm.py", "cwlrun", "cromwell", "rnaseq-workflow"])
