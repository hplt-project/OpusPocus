import logging

from opuspocus.pipeline_steps import build_step, OpusPocusStep, register_step


logger = logging.getLogger(__name__)


@register_step("debug")
class DebugStep(OpusPocusStep):

    def __init__(
        self,
        step,
        args,
    ):
        super().__init__(step, args)
        self.seed = args.seed
        self.text = args.text

    def build_dependencies(self, args):
        return {}

    @property
    def step_name(self):
        return "s.{}.1234".format(self.step)

    def init_step(self):
        super().init_step()

        # hard-link to the training corpus
        #os.link()

    def get_command_str(self) -> str:
        return 'echo {}'.format(self.text)
