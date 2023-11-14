import logging

from opuspocus.pipeline_steps import build_step, OpusPocusStep, register_step


logger = logging.getLogger(__name__)


@register_step('generate_vocab')
class GenerateVocabStep(OpusPocusStep):

    def __init__(
        self,
        args,
        src_lang: str,
        tgt_lang: str,
        step_name: str = None,
    ):
        self.seed = args.seed

        self.src_lang = src_lang
        self.tgt_lang = tgt_lang

        self.vocab_size = args.vocab_size

        self.spm_train = Path(args.marian_root, 'bin', 'spm_train')
        if self.spm_train.exists():
            raise ValueError(
                '[{}] File {} does not exist.'.format(self.step, self.spm_train)
            )

        super().__init__(args)

    def build_dependencies(self, args):
        return {
            'gather_train': build_step(
                'gather_train', args, monolingual=False
            )
        }

    @property
    def step_name(self):
        return 's.{}.{}-{}'.format(self.step, self.src_lang, self.tgt_lang)

    def init_step(self):
        super().init_step()

        # hard-link to the training corpus
        gather_step_dir = self.dependencies
        self.dependencies['gather_step']

    def get_cmd(self) -> str:
        return 'bash generate_vocab.sh ...'
