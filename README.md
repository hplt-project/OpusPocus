# OpusPocus

Modular NLP pipeline manager.

OpusPocus is aimed at simplifying the description and execution of popular and custom NLP pipelines, including dataset preprocessing, model training, fine-tuning and evaluation.
The pipeline manager supports execution using simple CLI (Bash) or common HPC schedulers (Slurm).

It uses [OpusCleaner](https://github.com/hplt-project/OpusCleaner/tree/main) for data preparation and [OpusTrainer](https://github.com/hplt-project/OpusTrainer) for training scheduling (development in progress).


## Structure

- `go.py` - pipeline manager entry script
- `opuspocus/` - OpusPocus modules
- `opuspocus_cli/` - OpusPocus CLI subcommands
- `config/` - default configuration files (pipeline config, marian training config, ...)
- `examples/` - pipeline manager usage examples
- `scripts/` - helper scripts, at this moment not directly implemented in OpusPocus
- `tests/` - unit tests


## Installation


1. Install [MarianNMT](https://marian-nmt.github.io/docs/)
```
./scripts/install_marian_gpu.sh PATH_TO_CUDA CUDNN_VERSION [NUM_THREADS]
```
2. Create a virtual environment for  [OpusCleaner](https://github.com/hplt-project/OpusCleaner/blob/main/README.md#installation-for-cleaning) and install it (at the moment, OpusCleaner has conflicting dependencies with OpusTrainer, therefore, has to be in an isolated Python environment).
3. Create another virtual environment for OpusPocus and install the OpusPocus.
```
pip install --upgrade pip setuptools
pip install -r requirements.txt
```

## Usage (Simple Pipeline)

Either run the main script `go.py` or the subcommand scripts from `opuspocus_cli/` directory.
Run the scripts directly from the root directory for this repository.

_Barry: I find that I still need to set my `PYTHONPATH` in the environment, otherwise all scripts fail because opuspocus cannot be imported._

# Data preparation

TODO: setup `data/` dir (modify the config/pipeline... to work with this directory

# Pipeline execution

There are two main subcommands (init, run) which need to be executed separately.
`./go.py init` prepares the pipeline directory structure and infers basic information about the datasets used in the pipeline.
`./go.py run` executes the pipeline graph, running the code from each of the pipeline step in the order defined by the pipeline graph.

(See the ``examples/`` directory for example execution)

# Data preprocessing example

1. Initialize the (data preprocessing) pipeline.
```
$ ./go.py init \
    --pipeline-config config/pipeline.preprocess.yml \
    --pipeline-dir preprocess_pipeline/destination/directory \
```
- `--pipeline-config` (required) provides the details about the pipeline steps and their dependencies
- `--pipeline-dir` (optional) overrides the `pipeline.pipeline_dir` value from the pipeline-config

2. Execute the (data preprocessing) pipeline.
```
$ ./go.py run \
    --pipeline-dir preprocess_pipeline/destination/directory \
    --runner bash \
```
- `--pipeline-dir` (required) path to the initialized pipeline directory.
- `--runner` (required) runner to be used for pipeline execution.

3. Check the pipeline status.
```
$ ./go.py traceback --pipeline-dir pipeline/destination/directory
```
OR
```
$ ./go.py status --pipeline-dir pipeline/destination/directory
```

# Model training example

(The data preprocessing pipeline must be finished, i.e. all steps must be in the DONE step)

0. Edit the location (TODO) of the preprocessing pipeline and directories

1. Initialize the (data preprocessing) pipeline.
```
$ ./go.py init \
    --pipeline-config config/pipeline.train.simple.yml \
    --pipeline-dir training_pipeline/destination/directory \
```

2. Execute the (data preprocessing) pipeline.
```
$ ./go.py run \
    --pipeline-dir training_pipeline/destination/directory \
    --runner bash \
```

# (Advanced) Config modification examples

1. Preprocessing your own data

TODO

2. Using own (preprocessed) data

TODO
