import argparse
import sys
import yaml
from pathlib import Path

from opuspocus.pipeline_steps import build_step, load_step
from opuspocus.utils import update_args


def build_pipeline(args, fn=build_step):
    """Initial idea of pipeline building."""
    # TODO: move into an object
    # TODO: refactor (?)
    # TODO: sort the steps dict to represent DAG (?)
    steps = {}

    # Clean para
    steps['clean_para'] = fn(
        'clean_para',
        args,
        src_lang=args.src_lang,
        tgt_lang=args.tgt_lang
    )

    # Decontaminate para using test
    steps['decontaminate_para'] = fn(
        'decontaminate_para',
        args,
        corpus_step=steps['clean_para']
    )

    # Gather para
    steps['gather_train.0'] = fn(
        'gather_train',
        args,
        suffix='iter-0',
        corpus_step=steps['decontaminate_para']
    )

    # Train BPE
    steps['generate_vocab'] = fn(
        'generate_vocab',
        args,
        corpus_step=steps['gather_train_0']
    )

    for lang in [args.src_lang, args.tgt_lang]:
        pass
        # Clean mono
        #steps["clean_mono.{}".format(lang)] = CleanStep(config, language=lang)

        # Gather mono
        #steps["gather_train_mono.{}".format(lang)] = GatherTrainStep(
        #    config,
        #    clean_step=steps["clean_mono.{}".format(lang)],
        #    language=lang
        #)

        # Split mono (for backtranslation)
        #steps["split_mono.{}".format(lang)] = SplitDatasetStep(
        #    config,
        #    gather_step=steps["gather_train_mono.{}".format(lang)],
        #    monolingual=True
        #)


    ## Training (iter N) ##

    # Iter 0 training (no BT data)
    for (src, tgt) in [(args.src, args.tgt), (args.tgt, args.src)]:
        steps['train.{}-{}.{}'.format(src, tgt, 0)] = fn(
            'train_model',
            args,
            iteration=0,
            vocab_step=steps['generate_vocab'],
            train_corpus_step=steps['gather_train.0'],
            model_init_step=None,
        )

    for i in range(args.bt_iterations):
        for (src, tgt) in [(config.src, config.tgt), (config.tgt, config.src)]:
            pass
            # Eval
            #steps["eval.{}-{}.{}".format(src, tgt, i)] = EvalStep(
            #    config,
            #    train_step=steps["train.{}-{}.{}".format(src, tgt, i)],
            #    iter=args.bt_iterations
            #)

            # Backtranslation
            #steps["translate.{}.{}".format(tgt, i)] = TranslateStep(
            #    config,
            #    split_mono_step=steps["split_mono.{}".format(tgt)],
            #    train_step=steps["train.{}-{}.{}".format(tgt, src, i)],
            #    tgt_lang=src,
            #    merge_output=True,
            #    iter=i
            #)

            # Clean BT
            #steps["clean_bt.{}.{}".format(tgt, i)] = CleanBT(
            #    config,
            #    gather_train_mono_step=steps["gather_train_mono.{}".format(tgt)],
            #    translate_step=steps["translate.{}.{}".format(tgt, i)],
            #    iter=i
            #)

            # Iter-i training
            #steps["train.{}-{}.{}".format(src, tgt, (i + 1))] = TrainStep(
            #    config,
            #    data_para_step=steps["gather_train_para"],
            #    data_synthetic_step=steps["clean_bt.{}.{}".format(tgt, i)],
            #    generate_vocab_step=steps["generate_vocab"],
            #    iter=(i+1)
            #)

    # TODO: Student training?

    return steps


def init_pipeline(pipeline):
    for _, v in pipeline.items()
        v.init_step()


def run_pipelinne(pipeline):
    for _, v in pipeline.items()
        v.run_step()


def traceback_pipeline(pipeline):
    for _, v in pipeline.items()
        v.traceback_step()


def main_init(args):
    pipeline = build_pipeline(args)
    init_pipeline(pipeline, args)

    # Dry-run
    if args.dry_run:
        # TODO: test 3rd party tools (?), anything else(?), touch files for
        # checking dependencies (?)
        pass


def main_run(args):
    pipeline = load_pipeline(args)
    run_pipeline(pipeline, args)


def main_traceback(args):
    pipeline = load_pipeline(args)
    traceback_pipeline(pipeline)


def main_list_commands(args):
    print(
       'Error: No command specified.\n\n'
       'Available commands:\n'
       '  init      initialize the pipeline\n'
       '  run       execute the pipeline\n'
       '  traceback print the pipeline graph\n'
       '', file=sys.stderr
    )
    sys.exit(1)



def create_parse_args():
    parser = argparse.ArgumentParser(description='TODO')
    parser.set_defaults(fn=main_list_commands)
    subparsers = parser.add_subparsers(help='command', dest='command')

    # TODO: more arguments (?)

    # Pipeline Init
    parser_init = subparsers.add_parser('init')
    parser_init.add_argument(
        '--config', type=str, required=None,
        help='Pipeline configuration JSON.'
    )
    parser_init.add_argument(
        '--vars', type=str, default='{}',
        help='Variable overwrite.'
    )
    parser_init.add_argument(
        '--dry-run', action='store_true',
        help='TODO'
    )
    parser_init.set_defaults(fn=main_init)

    # Pipeline Run
    parser_run = subparsers.add_parser('run')
    parser_run.add_argument(
        '--pipeline-dir', type=str, required=True,
        help='TODO'
    )
    parser_run.add_argument(
        '--runner', choices=['sbatch'], defaults='sbatch',
        help='TODO'
    )
    parser_run.add_argument(
        '--runner-opts', type=str, default=None,
        help='TODO'
    )
    parser_run.add_argument(
        '--overwrite', action='store_true',
        help='TODO'
    )
    parser_run.set_defaults(
        '--rerun-failed', action='store_true',
        help='TODO'
    )
    parser_run.set_defaults(fn=main_run)

    # Pipeline Traceback
    parser_traceback = subparsers.add_parser('traceback')
    parser_traceback.add_argument(
        '--pipeline_dir', type=str, required=True,
        help='TODO'
    )
    parser_traceback.add_argument(
        '--full', action='store_true',
        help='TODO'
    )
    parser_traceback.set_defaults(fn=main_traceback)

    return parser


def parse_args(parser):
    args = parser.parse_args()
    if args.command == 'init' and args.config is not None:
        config_path = Path(args.config).resolve()
        if not config_path.exists():
            raise ValueError('File {} not found.'.format(config_path))
        config = yaml.load(open(str(config_path), 'r'))
        delattr(args, 'config')
        return update_args(args, config)
    return args


if __name__ == '__main__':
    parser = create_args_parser()
    args = parse_args(parser)
    args.fn(args)
