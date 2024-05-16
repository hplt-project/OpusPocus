#!/usr/bin/env python3
import argparse
import sys


class Counter:
    seen = 0
    kept = 0
    removed = 0


def hash_mono(line):
    return line.strip().lower()


def make_hashes(line):
    src, tgt = line.split('\t', 2)
    # maybe we want to translate(str.maketrans("", "", string.punctuation))
    return hash_mono(src), hash_mono(tgt)


def main(args):
    src_test_samples = dict()
    tgt_test_samples = dict()
    removed = 0
    retained = 0

    for testfile in args.testfiles:
        for line in testfile:
            if args.mono:
                src = hash_mono(line)
                tgt = 'null'
            else:
                src, tgt = make_hashes(line)
            # if src in src_test_samples:
            #     print(f"'{src}' already appears on source-side", file=sys.stderr)
            # if tgt in tgt_test_samples:
            #     print(f"'{tgt}' already appears on target-side", file=sys.stderr)

            src_test_samples[src] = Counter()
            tgt_test_samples[tgt] = Counter()

    i = 1
    for line in sys.stdin:
        if args.mono:
            src = hash_mono(line)
            tgt = None
        else:
            src, tgt = make_hashes(line)

        # Seen
        src_seen, tgt_seen = False, False
        if src in src_test_samples:
            src_test_samples[src].seen += 1
            src_seen = True
        if not args.mono and tgt in tgt_test_samples:
            tgt_test_samples[tgt].seen +=1
            tgt_seen = True

        # Remove sentences which are present on either side of the devsets but
        # only if the average length is greater than min_length
        if src_seen or tgt_seen:
            if args.mono:
                limit = len(src) * 2
            else:
                limit = len(src) + len(tgt)

            if limit > 2 * args.min_length:
                if src in src_test_samples:
                    src_test_samples[src].removed += 1
                if not args.mono and tgt in tgt_test_samples:
                    tgt_test_samples[tgt].removed += 1
                removed += 1
                continue
            else:
                if src in src_test_samples:
                    src_test_samples[src].kept += 1
                if not args.mono and tgt in tgt_test_samples:
                    tgt_test_samples[tgt].kept += 1
                retained += 1
        i += 1

        # We never stripped the original newline
        print(line, end='')

    print(f'Removed {removed:,} lines out of {i:,}. Retained {retained:,} below length threshold', file=sys.stderr)

    for side, samples in [('Src', src_test_samples), ('Trg', tgt_test_samples)]:
        total_seen = sum(v.seen for v in samples.values())
        was_seen = sum(1 if v.seen > 0 else 0 for v in samples.values())

        print('Seen', file=sys.stderr)
        print(f'{side} total: {total_seen}/{i}', file=sys.stderr)
        print(f'{side} was: {was_seen/len(samples):%}', file=sys.stderr)

        total_removed = sum(v.removed for v in samples.values())
        was_removed = sum(1 if v.removed > 0 else 0 for v in samples.values())

        print('Removed', file=sys.stderr)
        print(f'{side} total: {total_removed}/{i}', file=sys.stderr)
        print(f'{side} was: {was_removed/len(samples):%}', file=sys.stderr)

        total_kept = sum(v.kept for v in samples.values())
        was_kept = sum(1 if v.kept > 0 else 0 for v in samples.values())

        print('Kept', file=sys.stderr)
        print(f'{side} total: {total_kept}/{i}', file=sys.stderr)
        print(f'{side} was: {was_kept/len(samples):%}', file=sys.stderr)
