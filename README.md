# OpusPocus on LUMI

Modular NLP pipeline manager.

OpusPocus is aimed at simplifying the description and execution of popular and custom NLP pipelines, including dataset preprocessing, model training and evaluation.
The pipeline manager supports execution using simple CLI (Bash) or common HPC schedulers (Slurm, HyperQueue).

It uses [OpusCleaner](https://github.com/hplt-project/OpusCleaner/tree/main) for data preparation and [OpusTrainer](https://github.com/hplt-project/OpusTrainer) for training scheduling (development in progress).


## Structure

- `go.py` - pipeline manager entry script
- `opuspocus/` - OpusPocus modules
- `config/` - default configuration files (pipeline config, marian training config, ...)
- `examples/` - pipeline manager usage examples
- `scripts/` - helper scripts, at this moment not directly implemented in OpusPocus
- `tests/` - unit tests


## Installation

1. Install [MarianNMT](https://marian-nmt.github.io/docs/).

2. Prepare the [OpusCleaner](https://github.com/hplt-project/OpusCleaner/blob/main/README.md#installation-for-cleaning) and [OpusTrainer](https://github.com/hplt-project/OpusTrainer/blob/main/README.md#installation) Python virtual environments.

3. Install the OpusPocus requirements.
```
pip install -r requirements.txt
```


## Usage (Simple Pipeline)

See the ``examples/`` directory for example execution

1. Initialize the pipeline.
```
$ ./go.py init \
    --pipeline-config path/to/pipeline/config/file \
    --pipeline-dir pipeline/destination/directory \
```

2. Execute the pipeline.
```
$ ./go.py run \
    --pipeline-dir pipeline/destination/directory \
    --runner bash \
```

3. Check the pipeline status.
```
$ ./go.py traceback --pipeline-dir pipeline/destination/directory
```
OR
```
$ ./go.py status --pipeline-dir pipeline/destination/directory
```
