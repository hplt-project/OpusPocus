#!/usr/bin/env python3

import argparse
# import string
import sys


def make_hashes(line):
    src, tgt = line.rstrip("\r").split("\t", 2)
    # maybe we want to translate(str.maketrans("", "", string.punctuation))
    return src.strip().lower(), tgt.strip().lower()


def main(args):

    src_test_samples = set()
    tgt_test_samples = set()
    removed = 0

    for testfile in args.testfiles:
        for line in testfile:
            src, tgt = make_hashes(line)
            src_test_samples.add(src)
            tgt_test_samples.add(tgt)

    for i, line in enumerate(sys.stdin):
        src, tgt = make_hashes(line)
        if src in src_test_samples or tgt in tgt_test_samples:
            if len(src) + len(tgt) > 2 * args.min_length:
                removed += 1
                continue

        print(line)

    print(f"Removed {removed} lines out of {i}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparser.ArgumentParser("decontamination script")
    parser.addArgument("testfiles", nargs="+", required=True,
                       type=argparse.FileType('r'))
    parser.addArgument("--min-length", required=False, type=int, default=0)
    args = parser.parse_args()

    main(args)
