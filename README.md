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

You can execute `./go.py --help` for general description or `./go.py <subcommand> --help` to list the available subcommand options.

# Pipeline execution

Run `./go.py run` (or `opuspocus_cli/run`) while providing a pipeline configuration file to execute a new pipeline:
```
$ ./go.py --pipeline-dir <pipeline_destination> --pipeline-donfig <config_file> --runner <runner>
```

Alternatively, run `./go.py run` while providing an existing pipeline directory to rerun a failed pipeline execution:
```
$ ./go.py run --pipeline-dir <pipeline_dir> --runner <runner>
```

You can use `--reinit` to reinitialize the exitisting pipeline before running.
You can use `--resubmit-done` to also execute pipeline steps in the DONE state.

Lastly, you can also stop and resubmit a running pipeline using `--stop-previous-run`
```
$ ./go.py run --pipeline-dir <pipeline_dir> --stop-previous-run
```

This is simialr to:
```
$ ./go.py stop --pipeline-dir <pipeline_dir>
$ ./go.py run --pipeline-dir <pipeline_dir>
```

# Other subcommands

- `stop` - stops the execution of a running pipeline
- `status`- prints the status of a pipeline its steps
- `traceback` - prints the dependency structure of a pipeline


## Examples

# I. Data preprocessing example

0. Download the data and setup the dataset directory structure.
```
$ scripts/prepare_data.en-eu.sh
```

1. Initialize and execute the (data preprocessing) pipeline.
```
$ mkdir -p experiments/en-eu/preprocess.simple
$ ./go.py run \
    --pipeline-config config/pipeline.preprocess.yml \
    --pipeline-dir experiments/en-eu/preprocess.simple \
	--runner bash
```
- `--pipeline-config` (required) provides the details about the pipeline steps and their dependencies
- `--pipeline-dir` (optional) overrides the `pipeline.pipeline_dir` value from the pipeline-config
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

1. Initialize and execute the training pipeline.
```
$ mkdir -p experiments/en-eu/train.simple
$ ./go.py run \
    --pipeline-config config/pipeline.train.simple.yml \
    --pipeline-dir experiments/en-eu/train.simple \
	--runner bash
```

# Acknowledgements

This project has received funding from the European Union’s Horizon Europe research and innovation programme under grant agreement No 101070350 and from UK Research and Innovation (UKRI) under the UK government’s Horizon Europe funding guarantee [grant number 10052546]
