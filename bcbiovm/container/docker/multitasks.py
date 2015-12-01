"""
Multiprocessing interface to run bcbio distributed functions inside
a local docker container.
"""
from bcbio import utils

from bcbiovm.container.docker import docker_container


@utils.map_wrap
def runfn(*args):
    """Exports processing of a specific function and arguments within
    docker using bcbio_nextgen.py runfn.

    Used primarily on re-runs of existing IPython distributed calls since
    local runs can happen fully inside of docker.

    Notes:
        Do not run checkpointed jobs externally now, as need to be inside
        docker container even if slow. We need a way to run a batch of
        jobs together.
        ::
            if parallel.get("checkpointed"):
            fn = getattr(multitasks, fn_name)
            return fn(fn_args)
    """
    container = docker_container.Docker()
    return container.run_function(function=args[0], docker_conf=args[1],
                                  cmd_args=args[2], parallel=args[3],
                                  arguments=args[4:])
