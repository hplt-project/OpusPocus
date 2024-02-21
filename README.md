# OpusPocus on LUMI

This branch is an implementation of the machine translation (MT) training pipeline manager for LUMI HPC cluster.
It uses [OpusCleaner](https://github.com/hplt-project/OpusCleaner/tree/main) for data preparation and [OpusTrainer](https://github.com/hplt-project/OpusTrainer) for training scheduling (in progress).


## Structure

- `go.py` - pipeline manager entry script
- `opuspocus/` - OpusPocus modules
- `config/` - default configuration files (pipeline config, marian training config, ...)
- `examples/` - pipeline manager usage examples
- `scripts/` - helper scripts, at this moment not directly implemented in OpusPocus


## Installation

1. Install [MarianNMT](https://marian-nmt.github.io/docs/).

2. Prepare the OpusCleaner and OpusTrainer Python virtual environments.

3. Install the OpusPocus requirements.
```
pip install -r requirements.txt
```


## Usage (Simple Pipeline)

You can see the example of the pipeline manager usage in examples directory.
Alternatively, you can follow these steps:

1. Initialize the pipeline.
```
python go.py init \
    --pipeline simple \
    --pipeline-dir pipeline/destination/directory \
    --pipeline-config path/to/pipeline/config/file \
    --src-lang en \
    --tgt-lang fr \
    --raw-data-dir training/corpora/directory \
    --valid-data-dir validation/data/directory \
    --test-data-dir test/data/directory \
    --marian-config path/to/marian/config/file \
```

(
The <training-corpora-dir> should contain the corpus .gz files, categories.json listing the corpora and their categories and (optional) the OpusCleaners .filter.json files.
The valid and test data dir should contain the parrallel validation corpora (plaintext).
Other pipeline parameters can be overwritten either by modifying the the pipeline config file (see the config/pipeline.* files) or by passing the parameter dicretly to the go.py command as a named argument.
)


2. Execute the pipeline.
```
python go.py run \
    --pipeline-dir pipeline/destination/directory \
    --runner sbatch \
    --runner-opts <options-for-runner> \
```

3. Check the pipeline status.
```
python go.py traceback --pipeline-dir pipeline/destination/directory
```
