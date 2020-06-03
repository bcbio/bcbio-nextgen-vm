import os
import shutil
import subprocess

import bcbio.utils
import pytest

from tests.conftest import install_cwl_test_files


def test_cwl_docker_somatic_workflow():
    with install_cwl_test_files() as workdir:
        with bcbio.utils.chdir(os.path.join(workdir, "somatic")):
            subprocess.check_call(["bash", "./run_generate_cwl.sh"])
            if os.path.exists("cromwell_work"):
                shutil.rmtree("cromwell_work")
            subprocess.check_call(["bcbio_vm.py", "cwlrun", "cromwell", "somatic-workflow"])


@pytest.mark.xfail(reason='https://github.com/bcbio/bcbio-nextgen-vm/issues/186')
def test_cwl_docker_joint_calling_workflow():
    with install_cwl_test_files() as work_dir:
        with bcbio.utils.chdir(os.path.join(work_dir, "gvcf_joint")):
            subprocess.check_call(["bash", "./run_generate_cwl.sh"])
            if os.path.exists("cromwell_work"):
                shutil.rmtree("cromwell_work")
            subprocess.check_call(["bash", "./run_cromwell.sh"])


def test_cwl_local_somatic_workflow(install_test_files):
    with install_cwl_test_files() as work_dir:
        with bcbio.utils.chdir(os.path.join(work_dir, "somatic")):
            subprocess.check_call(["bash", "./run_generate_cwl.sh"])
            if os.path.exists("cwltool_work"):
                shutil.rmtree("cwltool_work")
            subprocess.check_call(["bash", "./run_cwltool.sh"])


def test_cwl_rnaseq(install_test_files):
    with install_cwl_test_files() as work_dir:
        with bcbio.utils.chdir(os.path.join(work_dir, "rnaseq")):
            if os.path.exists("cromwell_work"):
                shutil.rmtree("cromwell_work")
            subprocess.check_call(["bcbio_vm.py", "cwlrun", "cromwell", "rnaseq-workflow"])


@pytest.mark.skipif(not (os.environ.get('ARVADOS_API_HOST')
                         and os.environ.get('ARVADOS_API_TOKEN')),
                    reason='Requires ARVADOS_API_HOST and ARVADOS_API_TOKEN')
@pytest.mark.xfail(reason='https://github.com/bcbio/bcbio-nextgen-vm/issues/187')
def test_cwl_arvados_workflow(install_test_files):
    with install_cwl_test_files() as work_dir:
        with bcbio.utils.chdir(os.path.join(work_dir, 'arvados')):
            subprocess.check_call(["bash", "./run_generate_cwl.sh"])
