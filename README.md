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
$ ./scripts/install_marian_gpu.sh PATH_TO_CUDA CUDNN_VERSION [NUM_THREADS]
```
Alternatively, you can usel `scripts/install_marian_cpu.sh` for CPU version. Note that the scripts may require modification based on your system.

2. (Optional) Setup the Python virtual environment (using virtualenv):
```
$ /usr/bin/virtualenv -p /usr/bin/python3.10 python-venv
```

3. Install the Python dependencies.
```
(source python-venv/bin/activate  # if using virtual environment)
$ pip install --upgrade pip setuptools
$ pip install -r requirements.txt
```

4. Setup the Python virtual environment for Opuscleaner. (OpusCleaner is currently not supported by Python>=3.10.)
```
$ /usr/bin/virtualenv -p /usr/bin/python3.9 opuscleaner-venv
```

5. Activate the OpusCleaner virtualenv and install OpusCleaner's dependencies
```
$ source opuscleaner-venv/bin/activate
$ pip install --upgrade pip setuptools
$ pip install -r requirements-opuscleaner.txt
```


## Usage (Simple Pipeline)

Either run the main script `go.py` or the subcommand scripts from `opuspocus_cli/` directory.
Run the scripts directly from the root directory for this repository.
(You may need to add the path to the local OpusPocus repository directory to your PYTHONPATH.)

# Pipeline execution

There are two main subcommands (init, run) which need to be executed separately.
`./go.py init` prepares the pipeline directory structure and infers basic information about the datasets used in the pipeline.
`./go.py run` executes the pipeline graph, running the code from each of the pipeline step in the order defined by the pipeline graph.

(See the ``examples/`` directory for example execution)

# I. Data preprocessing example

0. Download the data and setup the dataset directory structure.
```
$ scripts/prepare_data.en-eu.sh
```

1. Initialize the (data preprocessing) pipeline.
```
$ mkdir -p experiments/en-eu/preprocess.simple
$ ./go.py init \
    --pipeline-config config/pipeline.preprocess.yml \
    --pipeline-dir experiments/en-eu/preprocess.simple
```
- `--pipeline-config` (required) provides the details about the pipeline steps and their dependencies
- `--pipeline-dir` (optional) overrides the `pipeline.pipeline_dir` value from the pipeline-config

2. Execute the (data preprocessing) pipeline.
```
$ ./go.py run \
    --pipeline-dir experiments/en-eu/preprocess.simple \
    --runner bash 
```
- `--pipeline-dir` (required) path to the initialized pipeline directory.
- `--runner` (required) runner to be used for pipeline execution. Use --runner slurm for more effective HPC execution (if Slurm is available)

3. Check the pipeline status.
```
$ ./go.py traceback --pipeline-dir experiments/en-eu/preprocess.simple
```
OR
```
$ ./go.py status --pipeline-dir experiments/en-eu/preprocess.simple
```

# II. Model training example (preprocessing follow-up)

0. Check the preprocessing pipeline status (The data preprocessing pipeline must be finished, i.e. all steps must be in the DONE step)
```
$ ./go.py status --pipeline-dir experiments/en-eu/preprocess.simple
```

1. Initialize the training pipeline.
```
$ mkdir -p experiments/en-eu/train.simple
$ ./go.py init \
    --pipeline-config config/pipeline.train.simple.yml \
    --pipeline-dir experiments/en-eu/train.simple 
```

2. Execute the (data preprocessing) pipeline.
```
$ ./go.py run \
    --pipeline-dir experiments/en-eu/train.simple \
    --runner bash 
```

# (Advanced) Config modification examples

TBD
