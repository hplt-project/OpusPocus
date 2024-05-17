from typing import List

import logging
from pathlib import Path
from opuspocus.pipeline_steps import register_step
from opuspocus.pipeline_steps.corpus_step import CorpusStep
from opuspocus.pipeline_steps.opuspocus_step import OpusPocusStep
from opuspocus.utils import RunnerResources


logger = logging.getLogger(__name__)


@register_step('generate_vocab')
class GenerateVocabStep(OpusPocusStep):
    def __init__(
        self,
        step: str,
        step_label: str,
        pipeline_dir: Path,
        src_lang: str,
        tgt_lang: str,
        datasets: List[str],
        marian_dir: Path,
        corpus_step: CorpusStep,
        seed: int = 42,
        vocab_size: int = 64000,
    ):
        super().__init__(
            step=step,
            step_label=step_label,
            pipeline_dir=pipeline_dir,
            src_lang=src_lang,
            tgt_lang=tgt_lang,
            datasets=datasets,
            marian_dir=marian_dir,
            corpus_step=corpus_step,
            seed=seed,
            vocab_size=vocab_size,
        )

    def init_step(self) -> None:
        super().init_step()
        for dset in self.datasets:
            if dset not in self.corpus_step.dataset_list:
                raise ValueError(
                    'Dataset {} is not registered in the {} categories.json'
                    .format(dset, self.corpus_step.step_label)
                )

    @property
    def corpus_step(self) -> OpusPocusStep:
        return self.dependencies['corpus_step']

    @property
    def input_dir(self) -> Path:
        return self.corpus_step.output_dir

    def command(
        self,
        input_file: Path,
        runner: 'OpusPocusRunner'
    ) -> None:
        spm_train_path = Path(self.marian_dir, 'bin', 'spm_train')
        model_prefix = '{}/model.{}-{}'.format(
            self.output_dir, self.src_lang, self.tgt_lang
        )
        n_cpus = os.environ(RunnerResources.get_env_name('cpus'))

        train_datasets = ' '.join(
            '{}/{}.{}.gz'.format(self.input_dir, dset, lang)
            for dset in self.datasets for lang in self.languages
        )

        # Train subword model
        proc = subprocess.Popen(
            [
                str(spm_train_path),
                '--random_seed={}'.format(self.seed),
                '--bos_id=-1',
                '--eos_id=0',
                '--unk_id=1',
                '--model_prefix={}'.format(model_prefix),
                '--vocab_size={}'.format(self.vocab_size),
                '--input=<(cat {} | pigz -dc)'.format(train_datasets),
                '--input_sentence_size=10000000',
                '--shufle_input_sentence=true',
                '--train_extremely_large_corpus',
                '--byte_fallback',
                '--num_threads={}'.format(n_cpus)
            ]
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=os.environ,
            text=True
        )

        # Rename the output file
        shutil.move(model_prefix + '.model', model_prefix + '.spm')

        for suffix in ['spm', 'vocab']:
            os.symlink(
                'model.{}-{}.{}'.format(src_lang, tgt_lang, suffix),
                '{}/model.{}-{}.{}'.format(
                    self.output_dir, self.tgt_lang, self.src_lang, suffix
                )
            )
