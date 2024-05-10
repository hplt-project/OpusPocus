from typing import Dict, List, Optional, Tuple

import argparse
from pathlib import Path

from opuspocus import pipeline_steps
from opuspocus.pipelines import OpusPocusPipeline, register_pipeline
from opuspocus.utils import file_path

@register_pipeline('iterative_backtranslation')
class IterativeBacktranslationPipeline(OpusPocusPipeline):
    """Pipeline supporting the (iterative) backtranslation."""

    @staticmethod
    def add_args(parser):
        super(
            IterativeBacktranslationPipeline,
            IterativeBacktranslationPipeline
        ).add_args(parser)

        parser.add_argument(
            '--src-lang', type=str, required=True,
            help='source language'
        )
        parser.add_argument(
            '--tgt-lang', type=str, required=True,
            help='target language'
        )
        parser.add_argument(
            '--raw-data-parallel-dir', type=file_path, required=True,
            help='directory containing the parallel corpora with OpusCleaner '
                 'metadata (categories.json, filter.json files)'
        )
        parser.add_argument(
            '--raw-data-src-dir', type=file_path, required=True,
            help='directory containing the monolingual source-side corpora with '
                 'OpusCleaner\'s categories.json'
        )
        parser.add_argument(
            '--raw-data-tgt-dir', type=file_path, required=True,
            help='directory containing the monolingual target-side corpora with '
                 'OpusCleaner\'s categories.json'
        )
        parser.add_argument(
            '--skip-src-clean', action='store_true', default=False,
            help='skip the source-side monolingual corpura cleaning steps'
        )
        parser.add_argument(
            '--skip-tgt-clean', action='store_true', default=False,
            help='skip the target-side monolingual corpora cleaning steps'
        )
        parser.add_argument(
            '--valid-data-dir', type=file_path, required=True,
            help='directory containing the validation data'
        )
        parser.add_argument(
            '--test-data-dir', type=file_path, required=True,
            help='directory containing the final evaluation data'
        )
        parser.add_argument(
            '--opuscleaner-cmd', type=str, default='opuscleaner-clean',
            help='opuscleaner-clean command location'
        )
        parser.add_argument(
            '--decontaminate-path', type=file_path,
            default=Path('scripts/decontaminate.py'),
            help='path to the training data decontamination script'
        )
        parser.add_argument(
            '--python-venv-dir', type=file_path, required=True,
            help='path to the Python Conda environment'
        )
        parser.add_argument(
            '--marian-dir', type=file_path, required=True,
            help='path to the MarianNMT directory'
        )
        parser.add_argument(
            '--marian-config', type=file_path, required=True,
            help='path to the training config for MarianNMT'
        )
        parser.add_argument(
            '--opustrainer-config', type=file_path, default=None,
            help='path to the training config for OpusTrainer'
        )
        parser.add_argument(
            '--decontaminate-min-length', type=int, default=25,
            help='minimum length for the corpus decontamination step'
        )
        parser.add_argument(
            '--seed', type=int, default=42,
            help='fixed random seed'
        )
        parser.add_argument(
            '--vocab-size', type=int, default=64000,
            help='size of the translation model vocabulary'
        )
        parser.add_argument(
            '--best-model-suffix', type=str, default='best-chrf',
            help='suffix of the model used for backtranslation'
        )
        parser.add_argument(
            '--n-iterations', type=int, default=1,
            help='number of backtranslation iterations'
        )
        parser.add_argument(
            '--backtranslation-shard-size', type=int, default=None,
            help='corpus shard size for the backtranslation step parallelization'
        )

    def __init__(
        self,
        pipeline: str,
        args: argparse.Namespace,
        pipeline_dir: Optional[Path] = None,
        pipeline_config_path: Optional[Path] = None
    ):
        super().__init__(pipeline, args, pipeline_dir, pipeline_config_path)

    def build_pipeline_graph(self, args: argparse.Namespace):
        """Build the pipeline dependency graph for the pipeline instance."""

        steps = {}
        targets = []

        ## Preprocess parallel data ##

        # Collect raw corpora
        step_label = 'raw.{}-{}'.format(args.src_lang, args.tgt_lang)
        steps[step_label] = pipeline_steps.build_step(
            'raw',
            step_label=step_label,
            pipeline_dir=args.pipeline_dir,
            src_lang=args.src_lang,
            tgt_lang=args.tgt_lang,
            raw_data_dir=args.raw_data_parallel_dir,
        )

        # Cleaning
        step_label = 'clean.{}-{}'.format(args.src_lang, args.tgt_lang)
        steps[step_label] = pipeline_steps.build_step(
            'clean',
            step_label=step_label,
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
            step_label=step_label,
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
            step_label=step_label,
            pipeline_dir=args.pipeline_dir,
            src_lang=args.src_lang,
            tgt_lang=args.tgt_lang,
            python_venv_dir=args.python_venv_dir,
            previous_corpus_step=steps[
                'decontaminate.{}-{}'.format(args.src_lang, args.tgt_lang)
            ],
        )

        # Train BPE
        step_label = 'generate_vocab'
        steps[step_label] = pipeline_steps.build_step(
            'generate_vocab',
            step_label=step_label,
            pipeline_dir=args.pipeline_dir,
            src_lang=args.src_lang,
            tgt_lang=args.tgt_lang,
            datasets=['clean.{}-{}'.format(args.src_lang, args.tgt_lang)],
            marian_dir=args.marian_dir,
            corpus_step=steps[
                'gather.{}-{}'.format(args.src_lang, args.tgt_lang)
            ],
            seed=args.seed,
            vocab_size=args.vocab_size
        )

        ## Preprocess monolingual data ##

        # Collect raw corpora
        step_label = 'raw.{}'.format(args.src_lang)
        steps[step_label] = pipeline_steps.build_step(
            'raw',
            step_label=step_label,
            pipeline_dir=args.pipeline_dir,
            src_lang=args.src_lang,
            tgt_lang=None,
            raw_data_dir=args.raw_data_src_dir,
        )
        step_label = 'raw.{}'.format(args.tgt_lang)
        steps[step_label] = pipeline_steps.build_step(
            'raw',
            step_label=step_label,
            pipeline_dir=args.pipeline_dir,
            src_lang=args.tgt_lang,
            tgt_lang=None,
            raw_data_dir=args.raw_data_tgt_dir,
        )

        clean_langs = []
        if not args.skip_src_clean: clean_langs.append(args.src_lang)
        if not args.skip_tgt_clean: clean_langs.append(args.tgt_lang)
        for lang in clean_langs:
            # Clean the monolingual data
            step_label = 'clean.{}'.format(lang)
            steps[step_label] = pipeline_steps.build_step(
                'clean',
                step_label=step_label,
                pipeline_dir=args.pipeline_dir,
                src_lang=lang,
                tgt_lang=None,
                python_venv_dir=args.python_venv_dir,
                opuscleaner_cmd=args.opuscleaner_cmd,
                previous_corpus_step=steps[
                    'raw.{}'.format(lang)
                ]
            )

        for lang in [args.src_lang, args.tgt_lang]:
            # Remove test examples from train
            if 'clean.{}'.format(lang) in steps:
                prev_step = steps['clean.{}'.format(lang)]
            else:
                prev_step = steps['raw.{}'.format(lang)]
            step_label = 'decontaminate.{}'.format(lang)
            steps[step_label] = pipeline_steps.build_step(
                'decontaminate',
                step_label=step_label,
                pipeline_dir=args.pipeline_dir,
                src_lang=lang,
                tgt_lang=None,
                python_venv_dir=args.python_venv_dir,
                valid_data_dirs=[args.valid_data_dir, args.test_data_dir],
                previous_corpus_step=prev_step,
                decontaminate_path=args.decontaminate_path,
                min_length=args.decontaminate_min_length,
            )

            step_label = 'gather.{}'.format(lang)
            steps[step_label] = pipeline_steps.build_step(
                'gather',
                step_label=step_label,
                pipeline_dir=args.pipeline_dir,
                src_lang=lang,
                tgt_lang=None,
                python_venv_dir=args.python_venv_dir,
                previous_corpus_step=steps['decontaminate.{}'.format(lang)],
                output_shard_size=args.backtranslation_shard_size
            )

        ## Training ##

        # Training (0-th iteration, no BT data)
        for (src, tgt) in [(args.src_lang, args.tgt_lang), (args.tgt_lang, args.src_lang)]:
            step_label = 'train.{}-{}.0'.format(src, tgt)
            steps[step_label] = pipeline_steps.build_step(
                'train_model',
                step_label = step_label,
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
                train_category='clean',
                valid_dataset=args.valid_dataset,
            )

        # Traiing (i-th iteration, with BT data)
        for i in range(0, args.n_iterations):
            for (src, tgt) in [(args.src_lang, args.tgt_lang), (args.tgt_lang, args.src_lang)]:
                # Backtranslation
                step_label = 'translate.{}-{}.{}'.format(src, tgt, i)
                steps[step_label] = pipeline_steps.build_step(
                    'translate',
                    step_label=step_label,
                    pipeline_dir=args.pipeline_dir,
                    marian_dir=args.marian_dir,
                    src_lang=src,
                    tgt_lang=tgt,
                    previous_corpus_step=steps['gather.{}'.format(src)],
                    model_step=steps['train.{}-{}.{}'.format(src, tgt, i)],
                    model_suffix=args.best_model_suffix,
                )

                # TODO: cleaning

            for (src, tgt) in [(args.src_lang, args.tgt_lang), (args.tgt_lang, args.src_lang)]:
                # Combine the original and synthetic parallel data steps
                step_label = 'merge.{}-{}.{}'.format(src, tgt, i)
                steps[step_label] = pipeline_steps.build_step(
                    'merge',
                    step_label=step_label,
                    pipeline_dir=args.pipeline_dir,
                    src_lang=src,
                    tgt_lang=tgt,
                    previous_corpus_step=steps[
                        'gather.{}-{}'.format(args.src_lang, args.tgt_lang)
                    ],
                    previous_corpus_label='auth',
                    other_corpus_step=steps[
                        'translate.{}-{}.{}'.format(tgt, src, i)
                    ],
                    other_corpus_label='synth',
                )

                # Training
                # TODO: Use opustrainer and properly combine auth
                #       and synth data
                step_label = 'train.{}-{}.{}'.format(src, tgt, i + 1)
                steps[step_label] = pipeline_steps.build_step(
                    'train_model',
                    step_label=step_label,
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
                    train_category='clean',
                    valid_dataset=args.valid_dataset,
                )

        for (src, tgt) in [(args.src_lang, args.tgt_lang), (args.tgt_lang, args.src_lang)]:
            step_label = 'train.{}-{}.{}'.format(src, tgt, args.n_iterations)
            targets.append(steps[step_label])

        return steps, targets
