import logging
from opuspocus.pipeline_steps import (
    OpusPocusStep,
    build_step,
    register_step
)

logger = logging.getLogger(__name__)


@register_step('gather')
class GatherTrainStep(OpusPocusStep):
    def __init__(
        self,
        step,
        args,
        suffix: str,
        preprocessing_step: OpusPocusStep,
    ):
        super().__init__(step, args)
        self.dependencies = {
            'preprocessing_step': preprocessing_step,
        }

        self.src_lang = self.dependencies['preprocessing_step'].src_lang
        self.tgt_lang = self.dependencies['preprocessing_step'].tgt_lang

        self.suffix = suffix

    @property
    def step_name(self):
        return 's.{}.{}-{}.{}'.format(
            self.step,
            self.src_lang,
            self.tgt_lang,
            self.suffix
        )

    def get_command_str(self) -> str:
        """
            #!/usr/bin/env bash
            #SBATCH --job-name=gather_train
            #SBATCH --nodes=1
            #SBATCH --ntasks=1
            #SBATCH --cpus-per-task=1
            #SBATCH --mem=1G
            #SBATCH -o {logdir}/slurm.%j.log
            #SBATCH -e {logdir}/slurm.%j.log
            # Gather the individual datasets into training groups based
            # on the opuscleaner categories
            set -euo pipefail

            fail_and_rm() {
                echo $1 >&2
                [[ -e $2 ]] && rm -r $2
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

            categories_json="$INPUT_DIR/categories.json"
            categories=$(python -c "import json, sys; print(' '.join([x['name'] for x in json.load(open('$categories_json', 'r'))['categories']]))")
            for category in $categories; do
                for l in $SRC $TGT; do
                    datasets=$(python -c "import json, sys; print(''.join(json.load(open('$categories_json', 'r'))['mapping']['$category']))")
                    f_out="$OUTPUT_DIR/$category.para.$l.gz"
                    for dset in $datasets; do
                        ds_file=$INPUT_DIR/$dset.$l.gz
                        [[ ! -e "$ds_file" ]] \
                            && fail_and_rm "Missing $ds_file..." $f_out
                        echo "Adding $ds_file" >&2
                        cat $ds_file
                    done > $f_out
                done
                src_lines=$(zcat $OUTPUT_DIR/$category.para.$SRC.gz | wc -l)
                tgt_lines=$(zcat $OUTPUT_DIR/$category.para.$TGT.gz | wc -l)
                [[ $src_lines -ne $tgt_lines ]] \
                    && fail_and_rm "Lines in the output files (dataset $category) do not match ($src_lines != $tgt_lines)"
            done
        """.format(
            state_file=str(Path(self.step_dir, self.state_file)),
            src_lang=self.src_lang,
            tgt_lang=self.tgt_lang,
            indir=str(self.input_dir),
            outdir=str(self.output_dir),
            logdir=str(self.log_dir)
        )
