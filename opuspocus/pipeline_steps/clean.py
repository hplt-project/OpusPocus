import os
import glob
import logging
from pathlib import Path
from opuspocus.pipeline_steps import (
    OpusPocusStep,
    build_step,
    register_step
)

logger = logging.getLogger(__name__)


class CleanCorpusStep(OpusPocusStep):
    """
    TODO: split into individual corpus-steps
    TODO: reduce code duplicates from mono/para split

    """
    def __init__(
        self,
        step,
        args,
    ):
        super().__init__(step, args)
        self.opuscleaner_cmd = getattr(
            args, opuscleaner_cmd, 'opuscleaner-clean'
        )



@register_step('clean_para')
class CleanCorpusParaStep(CleanCorpusStep):
    def __init__(
        self,
        step,
        args,
        src_lang,
        tgt_lang,
    ):
        super().__init__(step, args)
        self.src_lang = src_lang
        self.tgt_lang = tgt_lang
        self.raw_data_dir = Path(args.raw_data_dir_para)
        if not self.raw_data_dir.exists():
            raise ValueError(
                'Directory {} does not exist.'.format(self.raw_data_dir)
            )

    @property
    def step_name(self):
        return 's.clean.{}-{}'.format(self.src_lang, self.tgt_lang)

    def get_command_str(self, args) -> str:
        return """
            #!/usr/bin/env bash
            #SBATCH --job-name=opuscleaner-clean
            #SBATCH --nodes=1
            #SBATCH --ntasks=1
            #SBATCH --cpus-per-task=8
            #SBATCH --mem=20G
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
            LOG_DIR="{logdir}"

            rm -r $OUTPUT_DIR/* $LOG_DIR/*
            for filter_file in $INPUT_DIR/*filters.json; do
                dataset=$(basename $filter_file)
                dataset=${{dataset/.filters.json/}}

                ## Run OpusCleaner ##
                echo "Cleaning $dataset..." >&2
                {opuscleaner} \
                    $filter_file \
                    --parallel $SLURM_CPUS_PER_TASK \
                    -b $INPUT_DIR \
                > >(
                    tee \
                        >(cut -f1 | gzip -c > $OUTPUT_DIR/$dataset.$SRC.gz) \
                        >(cut -f2 | gzip -c > $OUTPUT_DIR/$dataset.$TGT.gz) \
                        > /dev/null
                ) \
                2> >(tee $LOG_DIR/opuscleaner.$dataset.log >&2)

                # Validate Output
                src_lines=$(zcat $OUTPUT_DIR/$dataset.$SRC.gz | wc -l)
                tgt_lines=$(zcat $OUTPUT_DIR/$dataset.$TGT.gz | wc -l)
                [[ $src_lines -ne $tgt_lines ]] i\
                    && fail "Lines in the output files do not match ($src_lines != $tgt_lines)"
            done

            echo DONE > {state_file}
        """.format(
            state_file=str(Path(self.step_dir, self.state_file)),
            src_lang=self.src_lang,
            tgt_lang=self.tgt_lang,
            indir=str(self.raw_data_dir),
            outdir=str(self.output_dir),
            logdir=str(self.log_dir),
            opuscleaner=str(self.opuscleaner_cmd),
        )


@register_step('clean_mono')
class CleanCorpusMonoStep(CleanCorpusStep):
    def __init__(
        self,
        step,
        args,
        lang,
    ):
        super().__init__(step, args)
        self.lang = lang
        self.raw_data_dir = Path(
            getattr(args, 'raw_data_dir_{}'.format(self.lang))
        )
        if not self.raw_data_dir.exists():
            raise ValueError(
                'Directory {} does not exist.'.format(self.raw_data_dir)
            )


    @property
    def step_name(self):
        return 's.clean.{}'.format(self.lang)

    def get_command_str(self, args) -> str:
        return """
            #!/usr/bin/env bash
            #SBATCH --job-name=opuscleaner-clean
            #SBATCH --nodes=1
            #SBATCH --ntasks=1
            #SBATCH --cpus-per-task=8
            #SBATCH --mem=20G
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
            LOG_DIR="{logdir}"

            rm -r $OUTPUT_DIR/* $LOG_DIR/*
            for filter_file in $INPUT_DIR/*filters.json; do
                dataset=$(basename $filter_file)
                dataset=${{dataset/.filters.json/}}

                ## Run OpusCleaner ##
                echo "Cleaning $dataset..." >&2
                {opuscleaner} \
                    $filter_file \
                    --parallel $SLURM_CPUS_PER_TASK \
                    -b $INPUT_DIR \
                > >(
                    tee \
                        >(cut -f1 | gzip -c > $OUTPUT_DIR/$dataset.$LANG.gz) \
                        > /dev/null
                ) \
                2> >(tee $LOG_DIR/opuscleaner.$dataset.log >&2)
            done

            echo DONE > {state_file}
        """.format(
            state_file=str(Path(self.step_dir, self.state_file)),
            lang=self.lang,
            indir=str(self.raw_data_dir),
            outdir=str(self.output_dir),
            logdir=str(self.log_dir),
            opuscleaner=str(self.opuscleaner_cmd),
        )
