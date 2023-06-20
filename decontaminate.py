#!/usr/bin/env python3

import argparse
# import string
import sys


def make_hashes(sample):
    src, tgt = line.rstrip("\r").split("\t", 2)
    # maybe we want to translate(str.maketrans("", "", string.punctuation))
    return src.strip().lower(), tgt.strip().lower()


def main(args):

    src_test_samples = set()
    tgt_test_samples = set()

    for testfile in args.testfiles:
        for line in testfile:
            src, tgt = make_hashes(line)
            src_test_samples.add(make_hash(src))
            tgt_test_samples.add(make_hash(tgt))

    for line in sys.stdin:
        src, tgt = make_hashes(line)
        if src in src_test_samples or tgt in tgt_test_samples:
            if len(src) + len(tgt) > 2 * args.min_length:
                continue

        print(line)


if __name__ == "__main__":
    parser = argparser.ArgumentParser("decontamination script")
    parser.addArgument("testfiles", nargs="+", required=True,
                       type=argparse.FileType('r'))
    parser.addArgument("--min-length", required=False, type=int, default=0)
    args = parser.parse_args()

    main(args)
