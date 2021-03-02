import argparse


def niri_reduce_main():
    """
    Primary NIRI data processing entry point.
    """
    parser = argparse.ArgumentParser(description='NIRI data processor.')
    parser.add_argument('obsID', metavar='OBSID', type=str, nargs=1,
                        help='an observation ID to process')

    args = parser.parse_args()
    print(args)
