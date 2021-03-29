import argparse
import niriPipe.inttests
import niriPipe.utils.state
import logging
import os
import pkg_resources
import json
import glob


def _create_logger(verbose=False):
    root_logger = logging.getLogger('niriPipe')
    if verbose:
        root_logger.setLevel(logging.DEBUG)
    else:
        root_logger.setLevel(logging.INFO)
    root_logger.propagate = True
    if root_logger.hasHandlers:
        pass
    else:
        ch = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s')
        ch.setFormatter(formatter)
        root_logger.addHandler(ch)

    return root_logger


def run_main(args):
    """
    Run a NIRI pipeline.
    """

    if hasattr(args, 'verbose'):
        root_logger = _create_logger(verbose=True)
    else:
        root_logger = _create_logger()

    root_logger.info("Starting NIRI pipeline.")

    # Get initial state
    if hasattr(args, 'config') and args.config:
        configfile = args.config[0]
    else:
        configfile = pkg_resources.resource_filename(
            'niriPipe',
            os.path.join('cfg', 'default_config.cfg'))
    state = niriPipe.utils.state.get_initial_state(
        obs_name=args.obsID,
        configfile=configfile
    )
    root_logger.debug("Initial state:")
    root_logger.debug(json.dumps(state, sort_keys=True, indent=4))

    # Create and run finder
    root_logger.info(
        "Starting data finder for observation {}".format(
            state['current_stack']['obs_name']))
    try:
        finder = niriPipe.utils.finder.Finder(state)
        data_table = finder.run()
    except Exception as e:
        logging.critical("Datafinder failed!")
        raise e
    root_logger.info(
        "Finder succeeded; found {} files.".format(len(data_table)))

    # Run downloader on found files
    root_logger.info("Starting downloader.")
    try:
        downloader = \
            niriPipe.utils.downloader.Downloader(state=state, table=data_table)
        downloader.download_query_cadc()
    except Exception as e:
        logging.critical("Downloader failed!")
        raise e
    root_logger.info("Downloader succeeded.")

    # Check to see if a "stack" was created.
    if glob.glob("*_stack.fits"):
        root_logger.info(
            "Output stack found: {}".format(glob.glob("*_stack.fits")[0]))
    else:
        raise RuntimeError("Output stack not found.")

    root_logger.info("Pipeline finished!")


def niri_reduce_main():
    """
    Primary NIRI data processing entry point.
    """
    parser = argparse.ArgumentParser(description='NIRI data processor.')
    subparsers = parser.add_subparsers()

    parser_run = subparsers.add_parser('run')
    parser_run.add_argument('obsID', metavar='OBSID', type=str, nargs=1,
                            help='an observation ID to process')
    parser_run.add_argument('-c', '--config', type=str,
                            nargs=1, help='User provided config file.')
    parser_run.add_argument('-v', '--verbose', action='store_true',
                            help='Logs debug messages.')

    parser_test = subparsers.add_parser('test')
    parser_test.add_argument('testName', metavar='TESTNAME', type=str, nargs=1,
                             choices=['downloader', 'finder', 'run'],
                             help='Str name of test to run.')

    args = parser.parse_args()
    if hasattr(args, 'testName'):
        if 'downloader' in args.testName:
            niriPipe.inttests.downloader_inttest()
        elif 'finder' in args.testName:
            niriPipe.inttests.finder_inttest()
        elif 'run' in args.testName:
            niriPipe.inttests.run_inttest()
        else:
            raise ValueError("Invalid test name: {}".format(args.testName))
    elif hasattr(args, 'obsID'):
        run_main(args)
    else:
        parser.print_help()
