import argparse
import os
from collections import defaultdict
from typing import List, Dict, Any, Set, Optional, Union
from itertools import chain

import steps


def build_pipeline(config):
    """Initial idea of pipeline building."""
    # TODO: move into an object
    # TODO: refactor (?)
    # TODO: sort the steps dict to represent DAG (?)
    steps = {}

    # Clean para and filter test
    steps["clean_para"] = CleanStep(config)

    # Gather para
    steps["gather_train_para"] = GatherTrainStep(
        config,
        clean_step=steps["clean_para"]
    )

    # Train BPE
    steps["generate_vocab"] = GenerateVocabStep(
        config,
        gather_step=steps["gather_train_para"]
    )

    for lang in [config.src, config.tgt]:
        # Clean mono
        steps["clean_mono.{}".format(lang)] = CleanStep(config, language=lang)

        # Gather mono
        steps["gather_train_mono.{}".format(lang)] = GatherTrainStep(
            config,
            clean_step=steps["clean_mono.{}".format(lang)],
            language=lang
        )

        # Split mono (for backtranslation)
        steps["split_mono.{}".format(lang)] = SplitDatasetStep(
            config,
            gather_step=steps["gather_train_mono.{}".format(lang)],
            monolingual=True
        )


    ## Training (iter N) ##

    # Iter 0 training (no BT data)
    for (src, tgt) in [(config.src, config.tgt), (config.tgt, config.src)]:
        steps["train.{}-{}.{}".format(src, tgt, 0)] = TrainStep(
            config,
            data_para_step=steps["gather_train_para"],
            data_synthetic_step=None,
            generate_vocab_step=steps["generate_vocab"],
            src_lang=src,
            tgt_lang=tgt,
            iter=0
        )

    for i in range(args.bt_iterations):
        for (src, tgt) in [(config.src, config.tgt), (config.tgt, config.src)]:
            # Eval
            steps["eval.{}-{}.{}".format(src, tgt, i)] = EvalStep(
                config,
                train_step=steps["train.{}-{}.{}".format(src, tgt, i)],
                iter=args.bt_iterations
            )

            # Backtranslation
            steps["translate.{}.{}".format(tgt, i)] = TranslateStep(
                config,
                split_mono_step=steps["split_mono.{}".format(tgt)],
                train_step=steps["train.{}-{}.{}".format(tgt, src, i)],
                tgt_lang=src,
                merge_output=True,
                iter=i
            )

            # Clean BT
            steps["clean_bt.{}.{}".format(tgt, i)] = CleanBT(
                config,
                gather_train_mono_step=steps["gather_train_mono.{}".format(tgt)],
                translate_step=steps["translate.{}.{}".format(tgt, i)],
                iter=i
            )

            # Iter-i training
            steps["train.{}-{}.{}".format(src, tgt, (i + 1))] = TrainStep(
                config,
                data_para_step=steps["gather_train_para"],
                data_synthetic_step=steps["clean_bt.{}.{}".format(tgt, i)],
                generate_vocab_step=steps["generate_vocab"],
                iter=(i+1)
            )

    # TODO: Student training?

    return steps


def init_pipeline(pipeline):
    for _, v in pipeline.items()
        v.init()


def load_config(args):
    config = json.load(open(args.config))
    config_overwrite = json.loads(args.vars)

    for k, v in config_overwrite_json.items()
        # TODO: logging + what to do if overwrite is not in the config
        config[k] = v

    return config


def main_init(args):
    config = load_config(args)

    pipeline = build_pipeline(config)
    init_pipeline(config)

    # Save config
    json.save

    # Dry-run
    if args.dry_run:
        # TODO: test 3rd party tools (?), anything else (?)
        pass


def main_run(args):
    config = load_config(args)

    pipeline = build_pipeline(config)
    for _, v in pipeline.items():

        v.run()


def main_traceback(args):
    config = load_config(args)

    pipeline = build_pipeline(config)
    raise NotImplementedError()

    for _, v in pipeline.items()
        # TODO: sort DAG, print steps (with variable config)
        # [...]
        #v.traceback(args.full)


def main_list_commands(args):
    print(
        "Error: No command specified.\n\n"
        "Available commands:\n"
        "  init      initialize the pipeline\n"
        "  run       TODO\n"
        "  traceback TODO\n"
        "  TODO      other commands ?\n"
        "", file=sys.stderr
    )
    sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(description="TODO")
    parser.set_defaults(fn=main_list_commands)
    subparsers = parser.add_subparsers()

    # TODO: more arguments (?)

    # Pipeline Init
    parser_init = subparsers.add_parser("init")
    parser_init.add_argument(
        "--config", type=str, required=True,
        help="Pipeline configuration JSON."
    )
    parser_init.add_argument(
        "--vars", type=str, default="{}",
        help="Variable overwrite."
    )
    parser_init.add_argument(
        "--dry-run", action="store_true",
        help="TODO."
    )
    parser_init.set_defaults(fn=main_init)

    # Pipeline Run
    parser_run = subparsers.add_parser("run")
    parser_run.add_argument(
        "--pipeline_dir", type=str, required=True,
        help="TODO"
    )
    parser_run.add_argument(
        "--override", action="store_true",
        help="TODO"
    )
    parser_run.set_defaults(fn=main_run)

    # Pipeline Traceback
    parser_traceback = subparsers.add_parser("traceback")
    parser_traceback.add_argument(
        "--pipeline_dir", type=str, required=True,
        help="TODO"
    )
    parser_traceback.add_argument(
        "--full", action="store_true",
        help="TODO"
    )
    parser_traceback.set_defaults(fn=main_traceback)

    args = parser.parse_args()


if __name__ == "__main__":
    main(parse_args())
