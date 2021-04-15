import argparse
import niriPipe.inttests
import niriPipe.utils.state
import niriPipe.utils.customLogger
import niriPipe.utils.downloader
import niriPipe.utils.finder
import niriPipe.utils.reducer
import niriPipe.utils.tagger
import logging
import os
import json
import glob


module_logger = niriPipe.utils.customLogger.get_logger(__name__)


def run_main(args):
    """
    Run a NIRI pipeline.
    """
    if hasattr(args, 'verbose'):
        # Set niriPipe root logging level to DEBUG
        niriPipe.utils.customLogger.set_level(logging.DEBUG)

    module_logger.info("Starting NIRI pipeline.")

    # Get initial state
    if hasattr(args, 'config') and args.config:
        configfile = args.config
    else:
        configfile = None
    state = niriPipe.utils.state.get_initial_state(
        obs_name=args.obsID,
        intent=args.intent,
        configfile=configfile
    )
    module_logger.debug("Initial state:")
    module_logger.debug(json.dumps(state, sort_keys=True, indent=4))

    # Create and run finder
    module_logger.info(
        "Starting data finder for observation {}".format(
            state['current_stack']['obs_name']))
    try:
        finder = niriPipe.utils.finder.Finder(state)
        data_table = finder.run()
    except Exception as e:
        module_logger.critical("Datafinder failed!")
        raise e
    module_logger.info(
        "Finder succeeded; found {} files.".format(len(data_table)))

    # Run downloader on found files
    module_logger.info("Starting downloader.")
    try:
        downloader = \
            niriPipe.utils.downloader.Downloader(state=state, table=data_table)
        downloader.download_query_cadc()
    except Exception as e:
        module_logger.critical("Downloader failed!")
        raise e
    module_logger.info("Downloader succeeded.")

    # Setup and run Gemini DRAGONS.
    module_logger.info("Starting reducer.")
    try:
        reducer = niriPipe.utils.reducer.Reducer(state=state, table=data_table)
        products = reducer.run()
    except Exception as e:
        logging.critical("Reducer failed!")
        raise e
    module_logger.info("Reducer succeeded.")

    # Add/modify metadata for CADC
    module_logger.info("Starting Tagger.")
    try:
        tagger = niriPipe.utils.tagger.Tagger(products=products)
        tagger.run()
    except Exception as e:
        logging.critical("Tagger failed!")
        raise e
    module_logger.info("Tagger succeeded.")

    # Check to see if a "stack" was created.
    if glob.glob("*_stack.fits"):
        module_logger.info(
            "Output stack found: {}".format(glob.glob("*_stack.fits")[0]))
    else:
        raise RuntimeError("Output stack not found.")

    module_logger.info("Pipeline finished!")

    return products


def niri_reduce_main():
    """
    Primary NIRI data processing entry point.
    """
    parser = argparse.ArgumentParser(description='NIRI data processor.')
    subparsers = parser.add_subparsers()

    parser_run = subparsers.add_parser('run')
    parser_run.add_argument('obsID', metavar='OBSID', type=str, nargs=1,
                            help='an observation ID to process')
    parser_run.add_argument('intent', metavar='INTENT', type=str, nargs=1,
                            choices=['science', 'calibration'],
                            help='Type of stack (science or calibration).')
    parser_run.add_argument('-c', '--config', type=str,
                            nargs=1, help='User provided config file.')
    parser_run.add_argument('-v', '--verbose', action='store_true',
                            help='Logs debug messages.')

    parser_test = subparsers.add_parser('test')
    parser_test.add_argument('testName', metavar='TESTNAME', type=str, nargs=1,
                             choices=['downloader', 'finder', 'run', 'reduce'],
                             help='Str name of test to run.')

    args = parser.parse_args()
    if hasattr(args, 'testName'):
        if 'downloader' in args.testName:
            niriPipe.inttests.downloader_inttest()
        elif 'finder' in args.testName:
            niriPipe.inttests.finder_inttest()
        elif 'run' in args.testName:
            niriPipe.inttests.run_inttest()
        elif 'reduce' in args.testName:
            niriPipe.inttests.run_reduce_inttest()
        else:
            raise ValueError("Invalid test name: {}".format(args.testName))
    elif hasattr(args, 'obsID'):
        run_main(args)
    else:
        parser.print_help()
