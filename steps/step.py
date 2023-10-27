from pathlib import Path

import json


STATES = ["INITED", "RUNNING", "FAILED", "DONE"]


def get_hash():
    # TODO: proper hashing
    return "debug"


class PipelineStep:

    def __init__(self, args, step_instance=None):
        # 1. Create instance dir and related files
        # 2. Check existing dir and related files

        # create dir
        self.root_dir = args.root_dir

        self.step_instance = step_instance

        # dir exists?
        self._step_instance = self.create_instance(step_instance)


        if name is not None:
            self.dir = Path(self.root_dir, name)
        else:
            self.dir = self.__create_dir__

        self.dir = directory
        if directory is None:
            self.dir = create_dir(args.exp_root)
        else:
            self.dir = 

        # call deps, check deps
        self.dependencies = 
        # set status
        # get/set attributes
        # prepare cmd

        self.set_status()

    #@classmethod

    @property
    def step_name(self):
        return self.__class__._name__

    @property
    def step_instance(self):
        return self._step_instance

    @property
    def step_dir(self):
        return Path(self.root_dir, self.step_instance)

    @property
    def directory(self):
        return self.dir

    def create_instance(self):

        return "s." + self.step_name + get_hash()

    @property
    def dependencies(self):
        raise NotImplementedError

    def read_dependencies(self):
        deps_file = Path(self.dir, "step.dependencies")
        if not os.path.isfile(deps_file):
            return None
        return json.load(open(Path(self.dir, step.dependencies)))

    def write_dependencies(self):
       json.dumps(self.dependencies)

    def traceback(self):

    def init_step(self):
        pass

    def run_step(self):
        pass

    def load_state(self):
        self.state = open(Path(self.step_dir, "step.state"), "r")
            .readline().strip()

    def set_state(self, state):
        assert state in STATES
        self.state = state
        open(Path(self.step_dir, "step.state"), "w").writelines(self.state)

    def load_config(self):
        self.config = json.load(open(Path(self.step_dir, "step.config")))

    def save_config(self)

class HelloWorldStep(PipelineStep):
    def 
