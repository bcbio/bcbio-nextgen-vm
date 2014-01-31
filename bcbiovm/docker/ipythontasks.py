"""IPython interface to run bcbio distributed functions inside a docker container.

Exports processing of a specific function and arguments within docker using
bcbio_nextgen.py runfn.
"""
from IPython.parallel import require

from bcbio.distributed.ipythontasks import _setup_logging
from bcbiovm.docker import run

@require(run)
def runfn(*args):
    with _setup_logging(args):
        assert len(args) == 1
        fn_name = args[0][0]
        dockerconf = args[0][1]
        cmd_args = args[0][2]
        fn_args = args[0][3:]
        return run.do_runfn(fn_name, fn_args, cmd_args, dockerconf)
