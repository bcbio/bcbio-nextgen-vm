"""Run individual tasks within the Clusterk framework."""
import os
import shutil
import uuid

import yaml

from bcbio import utils
from bcbio.provenance import do

from bcbiovm.common import utils as common_utils
from bcbiovm.provider import factory as provider_factory


def _bootstrap_sh(fn_name, arg_file, parallel_file):
    template = (
        '# Bootstrap a bcbio-nextgen-vm installation on a bare Ubuntu machine'
        '# Targets recent Ubuntu versions (Ubuntu 13.10, Ubuntu 14.04)',
        '',
        '# Install Docker',
        ('sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys '
         '36A1D7869245C8950F966E92D8576A8BA88D21E9'),
        ('sudo sh -c "echo deb http://get.docker.io/ubuntu docker main >'
         ' /etc/apt/sources.list.d/docker.list"'),
        'sudo apt-get update',
        'sudo apt-get install lxc-docker',
        '',
        '# Install bcbio-nextgen-vm',
        'sudo apt-get install wget',
        'sudo mkdir /usr/local/share/bcbio-vm',
        'sudo chown $USER /usr/local/share/bcbio-vm',
        ('wget http://repo.continuum.io/miniconda/Miniconda-latest-'
         'Linux-x86_64.sh'),
        ('bash Miniconda-latest-Linux-x86_64.sh -b -p '
         '/usr/local/share/bcbio-vm/anaconda'),
        ('/usr/local/share/bcbio-vm/anaconda/bin/conda install --yes  '
         '-c https://conda.binstar.org/bcbio bcbio-nextgen-vm'),
        ('sudo ln -s /usr/local/share/bcbio-vm/anaconda/bin/bcbio_vm.py '
         '/usr/local/bin/bcbio_vm.py'),
        'sudo bcbio_vm.py install --tools',
        '',
        '# Run bcbio-vm',
        'bcbio_vm.py runfn {fn_name} {parallel_file} {arg_file}'.format(
            fn_name=fn_name, parallel_file=parallel_file,
            arg_file=arg_file),
    )
    return template


def _test_clusterk(fn_name, parallel_file, arg_file):
    """Do local runs of equivalent."""
    test_dir = utils.safe_makedir(os.path.expanduser("~/tmp/bcbiotest"))
    shutil.copy(parallel_file, test_dir)
    shutil.copy(arg_file, test_dir)
    with utils.chdir(test_dir):
        common_utils.execute(
            ["bcbio_vm.py", "runfn", fn_name, parallel_file, arg_file],
            check_exit_code=0)


def runfn(fn_name, queue, wrap_args, parallel, run_args, testing=True):
    """Run external function submitting to existing queue."""
    # FIXME(alexandrucoman): Unused argument 'wrap_args'
    # pylint: disable=unused-argument
    run_id = uuid.uuid4()
    aws_ship = provider_factory.get_ship("S3")
    shiping_config = provider_factory.get_ship_config("S3", raw=False)
    config = shiping_config(parallel["pack"])

    script_file = "bcbio-%s-%s-run.sh" % (fn_name, run_id)
    arg_file = "bcbio-%s-%s-args.json" % (fn_name, run_id)
    parallel_file = "bcbio-%s-%s-parallel.json" % (fn_name, run_id)
    tarball = "bcbio-%s-%s.tar.gz" % (fn_name, run_id)
    out_file = "%s-out%s" % os.path.splitext(arg_file)

    run_args = aws_ship.pack.send_run(run_args, config)
    with utils.chdir(os.getcwd()):
        with open(arg_file, "w") as out_handle:
            yaml.safe_dump(run_args, out_handle,
                           default_flow_style=False,
                           allow_unicode=False)
        with open(parallel_file, "w") as out_handle:
            yaml.safe_dump(parallel, out_handle,
                           default_flow_style=False,
                           allow_unicode=False)
        with open(script_file, "w") as out_handle:
            for line in _bootstrap_sh(fn_name, os.path.basename(arg_file),
                                      os.path.basename(parallel_file)):
                out_handle.writeline(line)

        if testing:
            _test_clusterk(fn_name, parallel_file, arg_file)
        else:
            do.run(["tar", "-czvpf", tarball, script_file, arg_file,
                    parallel_file],
                   "Prepare submission tarball")

            command = ["ksub.py",
                       "-q", queue["queue"],
                       "-e", str(int(float(queue["mem"]) * 1024)),
                       "-c", str(queue["cores_per_job"]),
                       "-u", os.path.abspath(tarball),
                       os.path.basename(script_file)]
            for tag in ("fnname=%s" % fn_name, ):
                command.extend(["--tag", tag])
            do.run(command, "Submit to clusterk")

        output_file = aws_ship.reconstitute.get_output(out_file, config)
        with open(output_file) as in_handle:
            out = yaml.safe_load(in_handle)
        for f in [script_file, parallel_file, arg_file, tarball, out_file]:
            if os.path.exists(f):
                os.remove(f)
    return out
