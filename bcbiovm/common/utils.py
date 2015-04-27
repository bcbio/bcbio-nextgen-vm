"""Utilities and helper functions."""

import logging
import sys

from bcbiovm.common import constant


def get_logger(name=constant.LOG.NAME, format_string=None):
    """Obtain a new logger object.

    :param name:          the name of the logger
    :param format_string: the format it will use for logging.

    If it is not given, the the one given at command
    line will be used, otherwise the default format.
    """
    logger = logging.getLogger(name)
    formatter = logging.Formatter(
        format_string or constant.LOG.FORMAT)

    if not logger.handlers:
        # If the logger wasn't obtained another time,
        # then it shouldn't have any loggers

        if constant.LOG.FILE:
            file_handler = logging.FileHandler(constant.LOG.FILE)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        logger.addHandler(stdout_handler)

    logger.setLevel(constant.LOG.LEVEL)
    return logger
