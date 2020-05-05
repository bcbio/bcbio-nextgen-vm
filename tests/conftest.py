import collections
import contextlib
from datetime import datetime
import io
import os
import shutil
import subprocess
import tarfile

import pytest
import requests

TEST_DIR = os.getenv('TRAVIS_BUILD_DIR', '/tmp/bcbio')


@contextlib.contextmanager
def install_cwl_test_files():
    orig_dir = os.getcwd()
    url = "https://github.com/bcbio/test_bcbio_cwl/archive/master.tar.gz"
    dirname = os.path.join(TEST_DIR, 'test_bcbio_cwl-master')
    if os.path.exists(dirname):
        # check for updated commits if the directory exists
        ctime = os.path.getctime(os.path.join(dirname, "README.md"))
        dtime = datetime.fromtimestamp(ctime).isoformat()
        r = requests.get("https://api.github.com/repos/bcbio/test_bcbio_cwl/commits?since=%s" % dtime).json()
        if len(r) > 0:
            shutil.rmtree(dirname)
    try:
        if not os.path.exists(dirname):
            print("Downloading CWL test directory: %s" % url)
            os.makedirs(dirname)
            os.chdir(os.path.dirname(dirname))
            r = requests.get(url)
            tf = tarfile.open(fileobj=io.BytesIO(r.content), mode='r|gz')
            tf.extractall()
        os.chdir(dirname)
        yield dirname
    finally:
        os.chdir(orig_dir)


@pytest.fixture
def install_test_files():
    """Download required sequence and reference files"""
    data_dir = os.path.join(TEST_DIR, 'automated')
    DlInfo = collections.namedtuple("DlInfo", "fname dirname version")
    download_data = [
        DlInfo("110106_FC70BUKAAXX.tar.gz", None, None),
        DlInfo("genomes_automated_test.tar.gz", "genomes", 34),
        DlInfo("110907_ERP000591.tar.gz", None, None),
        DlInfo("100326_FC6107FAAXX.tar.gz", None, 12),
        DlInfo("tcga_benchmark.tar.gz", None, 3),
        DlInfo("singlecell-rnaseq-test-data.tar.gz", "Harvard-inDrop", 1)
    ]
    for dl in download_data:
        url = "https://chapmanb.s3.amazonaws.com/{fname}".format(fname=dl.fname)
        dirname = os.path.join(data_dir, os.pardir,
                               dl.fname.replace(".tar.gz", "") if dl.dirname is None
                               else dl.dirname)
        if os.path.exists(dirname) and dl.version is not None:
            version_file = os.path.join(dirname, "VERSION")
            is_old = True
            if os.path.exists(version_file):
                with open(version_file) as in_handle:
                    version = int(in_handle.read())
                is_old = version < dl.version
            if is_old:
                shutil.rmtree(dirname)
        if not os.path.exists(dirname):
            _download_to_dir(url, dirname)


def _download_to_dir(url, dirname):
    subprocess.check_call(["wget", url])
    subprocess.check_call(["tar", "-xzvpf", os.path.basename(url)])
    shutil.move(os.path.basename(dirname), dirname)
    os.remove(os.path.basename(url))
