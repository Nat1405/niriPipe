import argparse
import niriPipe.inttests
import niriPipe.utils.state
import niriPipe.utils.customLogger
import niriPipe.utils.downloader
import niriPipe.utils.finder
import niriPipe.utils.reducer
import niriPipe.utils.tagger
import niriPipe.utils.checker
import logging
import json


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
        configfile=configfile,
        bandpass=args.bandpass
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
        tagger = niriPipe.utils.tagger.Tagger(state=state, products=products)
        products = tagger.run()
    except Exception as e:
        logging.critical("Tagger failed!")
        raise e
    module_logger.info("Tagger succeeded.")

    # Check to see if a "stack" was created.
    module_logger.info("Starting Checker.")
    try:
        checker = niriPipe.utils.checker.Checker(
            products=products, state=state)
        products = checker.run()
    except Exception as e:
        logging.critical("Checker failed!")
        raise e
    module_logger.info("Checker succeeded.")

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
    parser_run.add_argument('bandpass', metavar='BANDPASS', type=str, nargs=1,
                            choices=[
                                'Br(alpha)', 'Br(alpha)Con', 'Br(gamma)',
                                'CH4(long)', 'CH4(short)', 'CH4ice(2275)',
                                'CH4long_G0229', 'CH4short_G0228',
                                'CO_2-0_(bh)', 'Fell', 'H', 'H-con(157)',
                                'H20_ice', 'H20_ice(2045)', 'H2_1-0_S1',
                                'H2_2-1_S1', 'H2v=1-0s1&k',
                                'H2v=1-0s1G0216&K', 'H2v=2-1s1_G0220',
                                'H_order_sort', 'Hel', 'Hel(2p2s)',
                                'J', 'J_order_sort', 'Jcon(1065)',
                                'Jcon(112)', 'Jcon(121)', 'K', 'K(prime)',
                                'K(short)', 'K_order_sort', 'Kcon(209)',
                                'Kcon(227)', 'L(prime)', 'L_order_sort',
                                'M(prime)', 'M_order_sort', 'Pa(beta)',
                                'Y', 'hydrocarb'
                            ],
                            help='NIRI bandpass (filter) of stack.')
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
