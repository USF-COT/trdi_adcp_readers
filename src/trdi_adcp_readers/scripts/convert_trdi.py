#!/usr/bin/python

from trdi_adcp_readers.readers import (
    read_PD0_file,
    read_PD15_file
)

import argparse
from os import path
import sys

import pprint

ext_parser_map = {
    'pd0': read_PD0_file,
    'pd15': read_PD15_file
}


def main():
    parser = argparse.ArgumentParser(
        description="Converts a binary PD15 or PD0 TRDI ADCP file to CSV",
    )

    parser.add_argument(
        "--headers",
        default=0,
        help="Number of header lines to skip before looking for data",
        type=int
    )

    parser.add_argument(
        "--format",
        default=None,
        help="Binary format (pd15 or pd0) to convert from.  " +
             "Derives from file extension if not defined."
    )

    parser.add_argument(
        "input_file",
        help="file to be converted"
    )

    args = parser.parse_args()

    if args.format is None:
        (root, ext) = path.splitext(args.input_file)
        args.format = ext.lower()[1:]
    else:
        args.format = args.format.lower()

    if args.format in ext_parser_map:
        dataset = ext_parser_map[args.format](
            args.input_file,
            header_lines=args.headers
        )
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(dataset)
    else:
        raise ValueError(
            'Unrecognized extension %s found for input file' % args.format
        )


if __name__ == '__main__':
    sys.exit(main())
