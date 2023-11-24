import os
import glob
import logging
from pathlib import Path
from opuspocus.pipeline_steps import (
    CleanCorpusStep,
    CleanCorpusMonoStep,
    CleanCorpusParaStep,
    OpusPocusStep,
    build_step,
    register_step
)

logger = logging.getLogger(__name__)


class DecontaminateCorpusStep(OpusPocusStep):
    """
    TODO: split into individual corpus-steps
    TODO: reduce code duplicates from mono/para split
    """
    def __init__(
        self,
        step,
        args,
        clean_step: CleanCorpusStep,
    ):
        super().__init__(step, args)
        self.dependencies = {
            'clean_step': clean_step,
        }
        self.min_length = getattr(args, 'decontaminte_min_length', 25)

        self.valid_data_dir = Path(args.valid_data_dir)
        if not self.valid_data_dir.exists():
            raise ValueError(
                'Directory {} does not exist'.format(self.valid_data_dir)
            )

        self.decontaminate_script = Path(
            gettattr(args, 'decontaminate_script', 'scripts/decontaminate.py')
        )
        if not self.decontaminate_script.exists():
            raise ValueError(
                'File {} does not exist'.format(self.decontaminate_script)
            )


@register_step('decontaminate_para')
class DecontaminateCorpusParaStep(DecontaminateCorpusStep):
    def __init__(
        self,
        step,
        args,
        clean_step: CleanCorpusParaStep,
    ):
        super().__init__(step, args, clean_step)
        self.src_lang = self.dependencies['clean_step'].src_lang
        self.tgt_lang = self.dependencies['clean_step'].tgt_lang

    @property
    def step_name(self):
        return 's.{}.{}-{}'.format(
            self.step,
            self.src_lang,
            self.tgt_lang
        )

    def get_command_str(self, args) -> str:
        return """
            #!/usr/bin/env bash
            #SBATCH --job-name=decontaminate
            #SBATCH --nodes=1
            #SBATCH --ntasks=1
            #SBATCH --cpus-per-task=8
            #SBATCH --mem=5G
            #SBATCH -o {logdir}/slurm.%j.log
            #SBATCH -e {logdir}/slurm.%j.log
            # Preprocess and clean the data (mainly using opuscleaner)
            # TODO: replace non-parallel gzip with pigz
            set -euo pipefail

            fail() {
                echo $1 >&2
                exit 1
            }

            cleanup() {
                # Set the step state and exit
                echo FAILED > {state_file}
                exit $1
            }
            trap cleanup ALL

            SRC="{src_lang}"
            TGT="{tgt_lang}"

            INPUT_DIR="{indir}"
            OUTPUT_DIR="{outdir}"
            VALID_DIR="{valdir}"
            LOG_DIR="{logdir}"

            rm -r $OUTPUT_DIR/* $LOG_DIR/*
            for dataset in $INPUT_DIR/*.$SRC.gz; do
                dataset=$(basename $dataset)
                dataset=${{dataset/.$SRC.gz}}

                echo "Decontaminating $dataset..." >&2
                for dset in $VALID_DIR/*$SRC; do
                    path_prefix=${{dset/.$SRC/}}
                    [[ -e $path_prefix.$SRC-$TGT ]] \
                        || paste $path_prefix.$SRC $path_prefix.$TGT \
                            > $path_prefix.$SRC-$TGT
                done
                paste \
                    <(zcat $INPUT_DIR/$dataset.$SRC.gz) \
                    <(zcat $INPUT_DIR/$dataset.$TGT.gz) \
                | python {decontaminate} \
                    --min-length {min_length} \
                    $VALID_DIR/*$SRC-$TGT \
                > >(
                    tee \
                        >(cut -f1 | gzip -c > $OUTPUT_DIR/$dataset.$SRC.gz) \
                        >(cut -f2 | gzip -c > $OUTPUT_DIR/$dataset.$TGT.gz) \
                        > /dev/null
                )
                2> >(tee $LOG_DIR/decontaminate.$dataset.log >&2)

                # Validate Output
                src_lines=$(zcat $OUTPUT_DIR/$dataset.$SRC.gz | wc -l)
                tgt_lines=$(zcat $OUTPUT_DIR/$dataset.$TGT.gz | wc -l)
                [[ $src_lines -ne $tgt_lines ]] \
                        && fail "Lines in the output files do not match ($src_lines != $tgt_lines)"
            done

            # create link to the corpus categories file
            ln $INPUT_DIR/categories.json $OUTPUT_DIR/categories.json

            echo DONE > {state_file}
        """.format(
            state_file=str(Path(self.step_dir, self.state_file)),
            src_lang=self.src_lang,
            tgt_lang=self.tgt_lang,
            indir=str(self.input_dir),
            outdir=str(self.output_dir),
            valdir=str(self.valid_data_dir),
            logdir=str(self.log_dir),
            min_length=self.min_length,
            decontaminate=str(self.decontaminate_script),
        )


@register_step('decontaminate_mono')
class DecontaminateCorpusMonoStep(DecontaminateCorpusStep):
    def __init__(
        self,
        step,
        args,
        clean_step: CleanCorpusMonoStep,
    ):
        super().__init__(step, args, clean_step)
        self.lang = self.dependencies['clean_step'].lang

    @property
    def step_name(self):
        return 's.{}.{}'.format(self.step, self.lang)

    def get_command_str(self, args) -> str:
        return """
            #!/usr/bin/env bash
            #SBATCH --job-name=decontaminate
            #SBATCH --nodes=1
            #SBATCH --ntasks=1
            #SBATCH --cpus-per-task=8
            #SBATCH --mem=5G
            #SBATCH -o {logdir}/slurm.%j.log
            #SBATCH -e {logdir}/slurm.%j.log
            # Preprocess and clean the data (mainly using opuscleaner)
            # TODO: replace non-parallel gzip with pigz
            set -euo pipefail

            cleanup() {
                # Set the step state and exit
                echo FAILED > {state_file}
                exit $1
            }
            trap cleanup ALL

            LANG="{lang}"

            INPUT_DIR="{indir}"
            OUTPUT_DIR="{outdir}"
            VALID_DIR="{valdir}"
            LOG_DIR="{logdir}"

            rm -r $OUTPUT_DIR/* $LOG_DIR/*
            for dataset in $INPUT_DIR/*.$LANG.gz; do
                dataset=$(basename $dataset)
                dataset=${{dataset/.$LANG.gz}}

                echo "Decontaminating $dataset..." >&2
                zcat $INPUT_DIR/$dataset.$LANG.gz \
                | python {decontaminate} \
                    --min-length {min_length} \
                    $VALID_DIR/*$LANG \
                > >(
                    tee \
                        >(cut -f1 | gzip -c > $OUTPUT_DIR/$dataset.$LANG.gz) \
                        > /dev/null
                )
                2> >(tee $LOG_DIR/decontaminate.$dataset.log >&2)
            done

            # create link to the corpus categories file
            ln $INPUT_DIR/categories.json $OUTPUT_DIR/categories.json

            echo DONE > {state_file}
        """.format(
            state_file=str(Path(self.step_dir, self.state_file)),
            lang=self.lang,
            indir=str(self.input_dir),
            outdir=str(self.output_dir),
            valdir=str(self.valid_data_dir),
            logdir=str(self.log_dir),
            min_length=self.min_length,
            decontaminate=str(self.decontaminate_script),
        )
