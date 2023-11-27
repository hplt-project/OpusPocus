from typing import Dict, List, Tuple

import argparse

from opuspocus.pipelines import OpusPocusPipeline, register_pipeline
from opuspocus.pipelines.opuspocus_pipeline import STATE_T, TARGET_T
from opuspocus.pipeline_steps import OpusPocusStep, build_step


@register_pipeline('simple')
class SimplePipeline(OpusPocusPipeline):
    """Simple training pipeline. No backtranslation."""
    def __init__(
        self,
        pipeline: str,
        args: argparse.Namespace,
        steps: STATE_T = None,
        targets: TARGET_T = None,
    ):
        super().__init__(pipeline, args, steps, targets)

    def build_pipeline_graph(
        self, args: argparse.Namespace
    ) -> Tuple[STATE_T, TARGET_T]:
        steps = {}

        # Clean para
        steps['clean_para'] = build_step(
            'clean_para',
            args,
            src_lang=args.src_lang,
            tgt_lang=args.tgt_lang
        )

        # Decontaminate para using test
        steps['decontaminate_para'] = build_step(
            'decontaminate_para',
            args,
            corpus_step=steps['clean_para']
        )

        # Gather para
        steps['gather_train.0'] = build_step(
            'gather_train',
            args,
            suffix='iter-0',
            corpus_step=steps['decontaminate_para']
        )

        # Train BPE
        steps['generate_vocab'] = build_step(
            'generate_vocab',
            args,
            corpus_step=steps['gather_train_0']
        )

        # Iter 0 training (no BT data)
        for (src, tgt) in [(args.src, args.tgt), (args.tgt, args.src)]:
            steps['train.{}-{}.{}'.format(src, tgt, 0)] = build_step(
                'train_model',
                args,
                iteration=0,
                vocab_step=steps['generate_vocab'],
                train_corpus_step=steps['gather_train.0'],
                model_init_step=None,
            )

            targets = [steps['train.{}-{}.0'.format(args.src, args.tgt)],
            steps['train.{}-{}.0'.format(args.tgt, args.src)]]

        return steps, targets
