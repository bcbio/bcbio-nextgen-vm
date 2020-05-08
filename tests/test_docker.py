import os
import subprocess

from tests.conftest import data_dir, prepare_test_config, make_workdir


def test_analysis_in_docker_container(install_test_files):
    with make_workdir() as work_dir:
        subprocess.check_call([
            'bcbio_vm.py', f'--datadir={data_dir()}', 'run', '--image=quay.io/bcbio/bcbio-vc',
            f'--systemconfig={prepare_test_config(data_dir(), work_dir)}',
            f'--fcdir={os.path.join(data_dir(), os.pardir, "100326_FC6107FAAXX")}',
            os.path.join(data_dir(), 'run_info-bam.yaml')
        ])
