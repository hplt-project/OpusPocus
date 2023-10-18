# OpusPocus on LUMI

This branch is an implementation of the machine translation (MT) training pipeline for LUMI HPC cluster.
It uses [OpusCleaner](https://github.com/hplt-project/OpusCleaner/tree/main) for data preparation and [OpusTrainer](https://github.com/hplt-project/OpusTrainer) for training scheduling.


## Structure

- `go.sh` - main pipeline script
- `pipeline.*.sh` - pipeline configuration (paths, helper functions)]
- `config/` - configuration scripts (pipeline configuration, model settings,...)
- `scripts/` - smaller scripts
- `scripts/slurm/` - sbatch scripts substituting individual steps, should be run from the go.sh using sbatch and contain default SLURM parameters


## Usage

1. Modify the config/* files (directory structure, SLURM account/project number, model/training parameters, etc.)

2. Setup the experiment directory
```
sbatch bash get_data.sh <source_lang> <target_lang> <experiment_label>  # should be handled by this script

# Download (mono, para) corpora to data/raw/* directories.
# Download testsets to data/valid and data/test.
# Prepare the opuscleaner *.filters.json files in data/raw/para
```

3. Run the pipeline
```
bash go.sh <source_lang> <target_lang> <experiment_label>
```


## Current Status

- setting up individual pipeline steps as isolated scripts for manual execution (no fully working pipeline yet)


## TODO (Suggestions)
- semi-automate the corpus download, valid/test datasets dir setup
- skip steps that already produced their desired output
- strict separation of the main steps (data preparation, training, distillation...)
- ...

