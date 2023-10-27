import argparse
import os
from collections import defaultdict
from typing import List, Dict, Any, Set, Optional, Union
from itertools import chain


def create_dependencies(config_args):
    # TODO: Extend to data preparation, model testing, distillation
    # TODO: Make more robust, configurable
    # TODO: The logic should probably be moved to individual steps.
    #   Each step should list its own dependencies

    src = config["source"]
    tgt = config["target"]

    #para_categories = json.load(config_args["categories_para"])
    #mono_categories = json.load(config_args["categories_mono"])

    deps = {}

    # Download Data

    # Clean Data (opuscleaner)
    #dsets = []
    #for c in para_categories["categories"]:
    #    for ds in para_categories["mapping"][c["name"]]:
    #        dsets.append(ds)
    #        deps[f'clean_para.{ds}'] = None

    # Gather Para
    #deps["gather_para"] = [f'clean_para.{ds}' for ds in dsets]

    # Generate Vocab
    #deps["generate_vocab"] = ["gather_para"]

    # Clean and Gather Monolingual
    #dsets = []
    #for lang in [src, tgt]:
        # Clean
    #    for c in mono_categories["categories"]:
    #        for ds in mono_categories["mapping"][c["name"]]:
    #            dsets.append(ds)
    #            deps[f'clean_mono.{lang}.{ds}'] = None
        # Gather
    #    deps[f'gather_mono.{lang}'] = [f'clean_mono.{lang}.{ds}' for ds in dsets]

    # Train and BT
    for lp in [f'{src}-{tgt}', f'{tgt}-{src}']:
        lang_1, lang_2 = lp.split("-")
        for i in range(config_args["iter"]):
            if i == 0:
                deps[f'train.{lp}.{i}'] = ["generate_vocab"]
            else:
                prev_i = i - 1
                deps[f'train.{lp}.{i}'] = [f'clean_bt.{lang_2}-{lang_1}.{prev_i}']

            deps[f'backtranslate.{lp}.{i}'] = ["generate_vocab", f'train.{lp}.{i}', f'gather_mono.{lang_1}']
            deps[f'clean_bt.{lp}.{i}'] = [f'backtranslate.{lp}.{i}']
    return deps


def main_init(args):


    # Dry-run
    if args.dry_run:
        # ...

#def main_dryrun(args):


def main_run(args):


def main_tb(args):


def main_list_commands(args):
    print(
        "Error: No command specified.\n\n"
        "Available commands:\n"
        "  init      initialize the pipeline\n"
        "  run       TODO\n"
        "  tb        TODO\n"
        "  TODO      other commands ?\n"
        "", file=sys.stderr
    )
    sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(description="TODO")
    parser.set_defaults(fn=main_list_commands)
    subparsers = parser.add_subparsers()

    # TODO: individual arguments
    parser_init = subparsers.add_parser("init")
    parser.add_argument(
        "--config", type=str, required=True,
        help="Pipeline configuration JSON."
    )
    parser.add_argument(
        "--vars", type=str, default="{}",
        help="Variable overwrite."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="TODO."
    )
    parser_init.set_defaults(fn=main_init)

    parser_dryrun = subparsers.add_parser("dry-run")
    parser_dryrun.set_defaults(fn=main_dryrun)

    parser_run = subparsers.add_parser("run")
    parser_run.set_defaults(fn=main_run)

    #parser_tb = ...
    args = parser.parse_args()
    args(


if __name__ == "__main__":
    main(parse_args())
