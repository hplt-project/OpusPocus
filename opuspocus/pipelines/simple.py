from typing import Dict, List, Optional, Tuple

import argparse
from pathlib import Path

from opuspocus.pipeline_steps import OpusPocusStep
from opuspocus.pipelines import OpusPocusPipeline, register_pipeline
from opuspocus.utils import file_path

@register_pipeline('simple')
class SimplePipeline(OpusPocusPipeline):
    """A simple training pipeline containing no backtranslation."""

    @staticmethod
    def add_args(parser):
        super(SimplePipeline, SimplePipeline).add_args(parser)

        parser.add_argument(
            '--src-lang', type=str, required=True,
            help='source language'
        )
        parser.add_argument(
            '--tgt-lang', type=str, required=True,
            help='target language'
        )
        parser.add_argument(
            '--raw-data-dir', type=file_path, required=True,
            help='directory containing the parallel corpora with OpusCleaner '
                 'metadata (categories.json, filter.json files)'
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
            '--opuscleaner-python-venv-dir', type=file_path, required=True,
            help='path to the OpusCleaner Python Conda environment'
        )
        parser.add_argument(
            '--python-venv-dir', type=file_path, required=True,
            help='path to the Python Conda environmnent'
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
            '--opustrainer-config', type=file_path, required=True,
            help='path to the training config for OpusTrainer'
        )
        parser.add_argument(
            '--decontaminate-min-length', type=int, default=25,
            help='minimum length for the corpus decontamination step'
        )
        parser.add_argument(
            '--train_category', type=str, default='clean',
            help='training corpus category'
        )
        parser.add_argument(
            '--seed', type=int, default=42,
            help='fixed random seed'
        )
        parser.add_argument(
            '--vocab-size', type=int, default=64000,
            help='size of the translation model vocabulary'
        )

    def __init__(
        self,
        pipeline: str,
        args: argparse.Namespace,
        pipeline_dir: Optional[Path] = None,
        pipeline_config_path: Optional[Path] = None
    ):
        super().__init__(pipeline, args, pipeline_dir, pipeline_config_path)

    def _build_pipeline_graph(
        self,
        args: argparse.Namespace,
    ) -> Tuple[Dict[str, OpusPocusStep], List[str]]:
        """Build the pipeline dependency graph for the pipeline instance."""

        steps = {}
        targets = []

        # Load data
        step_label = 'raw.{}-{}'.format(args.src_lang, args.tgt_lang)
        steps[step_label] = pipeline_steps.build_step(
            'raw',
            step_label=step_label,
            pipeline_dir=args.pipeline_dir,
            src_lang=args.src_lang,
            tgt_lang=args.tgt_lang,
            raw_data_dir=args.raw_data_dir,
        )

        # Clean para
        step_label = 'clean.{}-{}'.format(args.src_lang, args.tgt_lang)
        steps[step_label] = pipeline_steps.build_step(
            'clean',
            step_label=step_label,
            pipeline_dir=args.pipeline_dir,
            src_lang=args.src_lang,
            tgt_lang=args.tgt_lang,
            previous_corpus_step=steps[
                'raw.{}-{}'.format(args.src_lang, args.tgt_lang)
            ],
            python_venv_dir=args.opuscleaner_python_venv_dir,
            opuscleaner_cmd=args.opuscleaner_cmd,
        )

        # Decontaminate para using test
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

        # Gather para
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

        # Training (no BT data)
        for (src, tgt) in [(args.src_lang, args.tgt_lang), (args.tgt_lang, args.src_lang)]:
            step_label = 'train.{}-{}'.format(src, tgt)
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
                    'gather.{}-{}'.format(args.src_lang, args.tgt_lang)
                ],
                model_init_step=None,
                seed=args.seed,
                train_category=args.train_category,
                valid_dataset=args.valid_dataset,
            )

            targets.append(step_label)

        return steps, targets
