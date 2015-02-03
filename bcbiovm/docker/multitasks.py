"""Multiprocessing interface to run bcbio distributed functions inside a local docker container.

Exports processing of a specific function and arguments within docker using
bcbio_nextgen.py runfn. Used primarily on re-runs of existing IPython distributed
calls since local runs can happen fully inside of docker.
"""
from bcbio import utils

from bcbio.distributed import multitasks
from bcbiovm.docker import run

@utils.map_wrap
def runfn(*args):
    fn_name = args[0]
    dockerconf = args[1]
    cmd_args = args[2]
    parallel = args[3]
    fn_args = args[4:]
    # Do not run checkpointed jobs externally now, as need to be inside docker
    # container even if slow. We need a way to run a batch of jobs together.
    #if parallel.get("checkpointed"):
    if False:
        fn = getattr(multitasks, fn_name)
        return fn(fn_args)
    else:
        return run.do_runfn(fn_name, fn_args, cmd_args, parallel, dockerconf)
