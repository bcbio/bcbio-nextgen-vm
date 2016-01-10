"""IPython interface to run bcbio distributed functions inside
a docker container.
"""
from ipyparallel import require
from bcbio.distributed import ipython
from bcbio.distributed.ipythontasks import _setup_logging

from bcbiovm import log as logging
from bcbiovm.container.docker import docker_container

LOG = logging.get_logger(__name__)


@require(docker_container)
def runfn(*args):
    """Exports processing of a specific function and arguments within docker
    using bcbio_nextgen.py runfn.
    """
    LOG.debug("Starting new worker with the following arguments: %s", args)
    container = docker_container.Docker()
    args = ipython.unzip_args(args)
    LOG.debug("Arguments after unzip: %s", args)
    assert len(args) == 1

    fn_args = args[0][4:]
    with _setup_logging(fn_args):
        return ipython.zip_args(
            container.run_function(function=args[0][0], docker_conf=args[0][1],
                                   cmd_args=args[0][2], parallel=args[0][3],
                                   arguments=fn_args))
