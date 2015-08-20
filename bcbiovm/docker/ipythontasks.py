"""IPython interface to run bcbio distributed functions inside
a docker container.

Exports processing of a specific function and arguments within docker using
bcbio_nextgen.py runfn.
"""
from ipyparallel import require

from bcbio.distributed import ipython
from bcbio.distributed.ipythontasks import _setup_logging
from bcbiovm.docker import run


@require(run)
def runfn(*args):
    args = ipython.unzip_args(args)
    assert len(args) == 1
    fn_args = args[0][4:]
    with _setup_logging(fn_args):
        fn_name = args[0][0]
        dockerconf = args[0][1]
        cmd_args = args[0][2]
        parallel = args[0][3]
        return ipython.zip_args(run.do_runfn(fn_name, fn_args, cmd_args,
                                             parallel, dockerconf))
