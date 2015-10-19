"""IPython interface to run bcbio distributed functions inside
a docker container.
"""
from ipyparallel import require
from bcbio.distributed import ipython
from bcbio.distributed.ipythontasks import _setup_logging

from bcbiovm.container.docker import docker_container


@require(docker_container)
def runfn(*args):
    """Exports processing of a specific function and arguments within docker
    using bcbio_nextgen.py runfn.
    """
    container = docker_container.Docker()
    args = ipython.unzip_args(args)
    assert len(args) == 1

    fn_args = args[0][4:]
    with _setup_logging(fn_args):
        return ipython.zip_args(container.run_function(function=args[0][0],
                                                       arguments=fn_args,
                                                       cmd_args=args[0][2],
                                                       parallel=args[0][3],
                                                       dockerconf=args[0][1]))
