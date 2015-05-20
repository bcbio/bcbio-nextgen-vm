"""Re-run individual tasks within multiprocessing framework."""
from __future__ import print_function


def runfn(*args):
    print(args)
    raise NotImplementedError
