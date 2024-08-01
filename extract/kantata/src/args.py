"""
This module args.py parses the command line arguments provided by the user.
"""

import argparse


def parse_arguments() -> argparse.Namespace:
    """
    The user can pass in the following required arguments:
        --reports

    """
    parser = argparse.ArgumentParser(
        description="kantata_extract command line arguments"
    )

    parser.add_argument(
        "-r",
        "--reports",
        nargs="+",
        type=str,
        help="The report name(s) to request from the API",
    )
    return parser.parse_args()
