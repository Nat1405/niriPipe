import argparse
import niriPipe.inttests


def run_main(*args, **kwargs):
    print("Recieved the following args:")
    for arg in args:
        print(arg)
    print("Recieved the following kwargs:")
    for arg in kwargs:
        print("{}: {}".format(arg, kwargs[arg]))


def niri_reduce_main():
    """
    Primary NIRI data processing entry point.
    """
    parser = argparse.ArgumentParser(description='NIRI data processor.')
    subparsers = parser.add_subparsers()

    parser_run = subparsers.add_parser('run')
    parser_run.add_argument('obsID', metavar='OBSID', type=str, nargs=1,
                            help='an observation ID to process')
    parser_run.set_defaults(func=run_main)

    parser_test = subparsers.add_parser('test')
    parser_test.add_argument('testName', metavar='TESTNAME', type=str, nargs=1,
                             choices=['downloader'],
                             help='Str name of test to run.')

    args = parser.parse_args()
    if hasattr(args, 'testName'):
        if 'downloader' in args.testName:
            niriPipe.inttests.downloader_inttest()
    else:
        parser.print_help()
