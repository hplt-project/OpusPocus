#!/usr/bin/env python3
import argparse
from opuspocus.tools.decontaminate import main


if __name__ == '__main__':
    parser = argparse.ArgumentParser('decontamination script')
    parser.add_argument('testfiles', nargs='+', type=argparse.FileType('r'))
    parser.add_argument('--min-length', required=False, type=int, default=0)
    parser.add_argument('--mono', action='store_true')
    args = parser.parse_args()

    main(args)
