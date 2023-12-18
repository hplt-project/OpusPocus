from typing import Dict, List, Tuple

import argparse
from pathlib import Path

from opuspocus import pipeline_steps
from opuspocus.pipelines import OpusPocusPipeline, register_pipeline
from opuspocus.utils import file_path

@register_pipeline('iterative_backtranslation')
class IterativeBacktranslationPipeline(OpusPocusPipeline):

    @staticmethod
    def add_args(parser):
        super(
            IterativeBacktranslationPipeline,
            IterativeBacktranslationPipeline
        ).add_args(parser)

        parser.add_argument(
            '--src-lang', type=str, required=True,
            help='TODO'
        )
        parser.add_argument(
            '--tgt-lang', type=str, required=True,
            help='TODO'
        )
        parser.add_argument(
            '--raw-data-parallel-dir', type=file_path, required=True,
            help='TODO'
        )
        parser.add_argument(
            '--raw-data-src-dir', type=file_path, required=True,
            help='TODO'
        )
        parser.add_argument(
            '--raw-data-tgt-dir', type=file_path, required=True,
            help='TODO'
        )
        parser.add_argument(
            '--valid-data-dir', type=file_path, required=True,
            help='TODO'
        )
        parser.add_argument(
            '--test-data-dir', type=file_path, required=True,
            help='TODO'
        )
        parser.add_argument(
            '--opuscleaner-cmd', type=str, default='opuscleaner-clean',
            help='TODO'
        )
        parser.add_argument(
            '--decontaminate-path', type=file_path,
            default=Path('scripts/decontaminate.py'),
            help='TODO'
        )
        parser.add_argument(
            '--python-venv-dir', type=file_path, required=True,
            help='TODO'
        )
        parser.add_argument(
            '--marian-dir', type=file_path, required=True,
            help='TODO'
        )
        parser.add_argument(
            '--marian-config', type=file_path, required=True,
            help='TODO'
        )
        parser.add_argument(
            '--opustrainer-config', type=file_path, required=True,
            help='TODO'
        )
        parser.add_argument(
            '--decontaminate-min-length', type=int, default=25,
            help='TODO'
        )
        parser.add_argument(
            '--seed', type=int, default=42,
            help='TODO'
        )
        parser.add_argument(
            '--vocab-size', type=int, default=64000,
            help='TODO'
        )
        parser.add_argument(
            '--best-model-suffix', type=str, default='best-chrf',
            help='TODO'
        )

    """Simple training pipeline. No backtranslation."""
    def __init__(
        self,
        pipeline: str,
        args: argparse.Namespace,
        steps = None,
        targets = None,
    ):
        super().__init__(pipeline, args, steps, targets)

    def build_pipeline_graph(self, args: argparse.Namespace):
        steps = {}
        targets = []

        ## Preprocess parallel data ##

        # Collect raw corpora
        step_label = 'raw.{}-{}'.format(args.src_lang, args.tgt_lang)
        steps[step_label] = pipeline_steps.build_step(
            'raw',
            pipeline_dir=args.pipeline_dir,
            src_lang=args.src_lang,
            tgt_lang=args.tgt_lang,
            raw_data_dir=args.raw_data_parallel_dir,
        )

        # Cleaning
        step_label = 'clean.{}-{}'.format(args.src_lang, args.tgt_lang)
        steps[step_label] = pipeline_steps.build_step(
            'clean',
            pipeline_dir=args.pipeline_dir,
            src_lang=args.src_lang,
            tgt_lang=args.tgt_lang,
            python_venv_dir=args.python_venv_dir,
            opuscleaner_cmd=args.opuscleaner_cmd,
            previous_corpus_step=steps[
                'raw.{}-{}'.format(args.src_lang, args.tgt_lang)
            ],
        )

        # Remove test examples from train
        step_label = 'decontaminate.{}-{}'.format(args.src_lang, args.tgt_lang)
        steps[step_label] = pipeline_steps.build_step(
            'decontaminate',
            pipeline_dir=args.pipeline_dir,
            src_lang=args.src_lang,
            tgt_lang=args.tgt_lang,
            python_venv_dir=args.python_venv_dir,
            valid_data_dirs=[args.valid_data_dir, args.test_data_dir],
            previous_corpus_step=steps[
                'clean.{}-{}'.format(args.src_lang, args.tgt_lang)
            ],
            decontaminate_path=args.decontaminate_path,
            min_length=args.decontaminate_min_length,
        )

        # Combine the various data sources
        step_label = 'gather.{}-{}'.format(args.src_lang, args.tgt_lang)
        steps[step_label] = pipeline_steps.build_step(
            'gather',
            pipeline_dir=args.pipeline_dir,
            src_lang=args.src_lang,
            tgt_lang=args.tgt_lang,
            previous_corpus_step=steps[
                'decontaminate.{}-{}'.format(args.src_lang, args.tgt_lang)
            ],
        )

        # Train BPE
        steps['generate_vocab'] = pipeline_steps.build_step(
            'generate_vocab',
            pipeline_dir=args.pipeline_dir,
            src_lang=args.src_lang,
            tgt_lang=args.tgt_lang,
            datasets=['clean.{}-{}'.format(args.src_lang, args.tgt_lang)],
            marian_dir=args.marian_dir,
            corpus_step=steps[
                'gather.{}-{}'.format(args.src_lang, arsg.tgt_lang)
            ],
            seed=args.seed,
            vocab_size=args.vocab_size
        )

        ## Preprocess monolingual data ##

        # Collect raw corpora
        steps['raw.{}'.format(args.src_lang)] = pipeline_steps.build_step(
            'raw',
            pipeline_dir=args.pipeline_dir,
            src_lang=args.src_lang,
            tgt_lang=None,
            raw_data_dir=args.raw_data_src_dir,
        )
        steps['raw.{}'.format(args.tgt_lang)] = pipeline_steps.build_step(
            'raw',
            pipeline_dir=args.pipeline_dir,
            src_lang=args.tgt_lang,
            tgt_lang=None,
            raw_data_dir=args.raw_data_tgt_dir,
        )

        for lang in [args.src_lang, args.tgt_lang]:
            # Clean the monolingual data
            steps['clean.{}'.format(args.lang)] = pipeline_steps.build_step(
                'clean',
                pipeline_dir=args.pipeline_dir,
                src_lang=lang,
                tgt_lang=None,
                python_venv_dir=args.python_venv_dir,
                opuscleaner_cmd=args.opuscleaner_cmd,
                previous_corpus_step=steps[
                    'raw.{}'.format(lang)
                ]
            )

            # Remove test examples from train
            steps['decontaminate.{}'.format(lang)] = pipeline_steps.build_step(
                'decontaminate',
                pipeline_dir=args.pipeline_dir,
                src_lang=lang,
                tgt_lang=None,
                python_venv_dir=args.python_venv_dir,
                valid_data_dirs=[args.valid_data_dir, args.test_data_dir],
                previous_corpus_step=steps['clean.{}'.format(lang)],
                decontaminate_path=args.decontaminate_path,
                min_length=args.decontaminate_min_length,
            )

            steps['gather.{}'.format(lang)] = pipeline_steps.build_step(
                'gather',
                pipeline_dir=args.pipeline_dir,
                src_lang=lang,
                tgt_lang=None,
                previous_corpus_step=steps['decontaminate.{}'.format(lang)],
            )

        ## Training ##

        # Training (0-th iteration, no BT data)
        for (src, tgt) in [(args.src_lang, args.tgt_lang), (args.tgt_lang, args.src_lang)]:
            step_label = 'train.{}-{}.0'.format(src, tgt)
            steps[step_label] = pipeline_steps.build_step(
                'train_model',
                pipeline_dir=args.pipeline_dir,
                src_lang=src,
                tgt_lang=tgt,
                marian_dir=args.marian_dir,
                python_venv_dir=args.python_venv_dir,
                valid_data_dir=args.valid_data_dir,
                marian_config=args.marian_config,
                opustrainer_config=args.opustrainer_config,
                vocab_step=steps['generate_vocab'],
                train_corpus_step=steps[
                    'gather.{}-{}'.format(args.src_lang, args.tgt_lang)
                ],
                model_init_step=None,
                seed=args.seed,
                train_dataset='clean.{}-{}'.format(
                    args.src_lang, args.tgt_lang
                ),
                valid_dataset=args.valid_dataset,
                suffix='iter-0'
            )

        # Traiing (i-th iteration, with BT data)
        for i in range(0, args.bt_iterations):
            for (src, tgt) in [(args.src_lang, args.tgt_lang), (args.tgt_lang, args.src_lang)]:
                # Backtranslation
                step_label = 'translate.{}-{}.{}'.format(src, tgt, i)
                steps[step_label] = pipeline_steps.build_step(
                    'translate',
                    pipeline_dir=args.pipeline_dir,
                    marian_dir=args.marian_dir,
                    src_lang=src,
                    tgt_lang=tgt,
                    previous_corpus_step=steps['gather.{}'.format(src)],
                    model_step=steps['train.{}-{}.{}'.format(src, tgt, i)],
                    model_suffix=args.best_model_suffix,
                    suffix='iter-{}'.format(i)
                )

                # TODO: cleaning

            for (src, tgt) in [(args.src_lang, args.tgt_lang), (args.tgt_lang, args.src_lang)]:
                # Combine the original and synthetic parallel data steps
                step_label = 'merge.{}-{}.{}'.format(src, tgt, i)
                steps[step_label] = pipeline_steps.build_step(
                    'merge',
                    pipeline_dir=args.pipeline_dir,
                    src_lang=src,
                    tgt_lang=tgt,
                    previous_corpus_step=steps['gather_para'],
                    previous_corpus_label='auth',
                    other_corpus_steps=steps[
                        'translate.{}-{}.{}'.format(tgt, src, i)
                    ],
                    other_corpus_label='synth',
                    suffix='iter-{}'.format(i)
                )

                # Training
                # TODO: Use opustrainer and properly combine auth
                #       and synth data
                step_label = 'train.{}-{}.{}'.format(src, tgt, i + 1)
                steps[step_label] = pipeline_steps.build_step(
                    'train_model',
                    pipeline_dir=args.pipeline_dir,
                    src_lang=src,
                    tgt_lang=tgt,
                    marian_dir=args.marian_dir,
                    python_venv_dir=args.python_venv_dir,
                    valid_data_dir=args.valid_data_dir,
                    marian_config=args.marian_config,
                    opustrainer_config=args.opustrainer_config,
                    vocab_step=steps['generate_vocab'],
                    train_corpus_step=steps[
                        'merge.{}-{}.{}'.format(src, tgt, i)
                    ],
                    model_init_step=steps[
                        'train.{}-{}.{}'.format(src, tgt, i)
                    ],
                    seed=args.seed,
                    train_dataset='clean.synth.{}-{}'.format(src, tgt),
                    valid_dataset=args.valid_dataset,
                    suffix='iter-{}'.format(i + 1)
                )

        for (src, tgt) in [(args.src_lang, args.tgt_lang), (args.tgt_lang, args.src_lang)]:
            step_label = 'train.{}-{}.{}'.format(src, tgt, args.bt_iterations)
            targets.append(steps[step_label])

        return steps, targets
