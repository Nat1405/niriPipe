import logging
"""
We need niriPipe to have a base 'niriPipe' logger that doesn't
inherit from the true root logger (because other modules seem
to mess around with the root logger).
This code is idempotent, and should be imported by each niriPipe module.
"""


def _create_root_logger():
    niriPipe_root_logger = logging.getLogger(__name__.split('.')[0])

    if not niriPipe_root_logger.hasHandlers():
        # Should only ever be run once.
        niriPipe_root_logger.propagate = False
        niriPipe_root_logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s')
        ch.setFormatter(formatter)
        niriPipe_root_logger.addHandler(ch)

    return niriPipe_root_logger


def get_logger(name):
    return logging.getLogger(name)


def set_level(level):
    niriPipe_root_logger.setLevel(level)


def enable_propagation():
    niriPipe_root_logger.propagate = True


niriPipe_root_logger = _create_root_logger()
