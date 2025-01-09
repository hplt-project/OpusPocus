#!/usr/bin/env python3
"""A translation model trainer. It feeds marian different sets of datasets with different thresholds
for different stages of the training. Data is uncompressed and TSV formatted src\ttrg
"""

import argparse
import logging
import signal
import subprocess
import sys
from pathlib import Path

import yaml
from opustrainer.trainer import (
    AsyncDatasetReader,
    CurriculumLoader,
    DatasetReader,
    StateTracker,
    Trainer,
    ignore_sigint,
    print_state,
)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Feeds marian tsv data for training.")
    parser.add_argument("--config", "-c", required=True, type=str, help="YAML configuration input.")
    parser.add_argument("--state", "-s", type=str, help="YAML state file, defaults to ${CONFIG}.state.")
    parser.add_argument("--sync", action="store_true", help="Do not shuffle async.")
    parser.add_argument(
        "--temporary-directory",
        "-T",
        default=None,
        type=str,
        help="Temporary dir, used for shuffling and tracking state.",
    )
    parser.add_argument(
        "--do-not-resume", "-d", action="store_true", help="Do not resume from the previous training state."
    )
    parser.add_argument(
        "--no-shuffle", "-n", action="store_false", dest="shuffle", help="Do not shuffle, for debugging."
    )
    parser.add_argument(
        "trainer",
        type=str,
        nargs=argparse.REMAINDER,
        help="Trainer program that gets fed the input. If empty it is read from config.",
    )

    return parser.parse_args()


def main(args: argparse.Namespace) -> int:
    config_path = Path(args.config)
    with config_path.open(encoding="utf-8") as fh:
        config = yaml.safe_load(fh)

    curriculum = CurriculumLoader().load(config, basepath=config_path.parent)

    # Quick cheap check that all files exist before we begin training
    for dataset in curriculum.datasets.values():
        missing_files = {file for file in dataset.files if not Path(file).exists()}
        if missing_files:
            err_msg = f"Dataset '{dataset.name}' is missing files: {missing_files}"
            raise ValueError(err_msg)

    trainer = Trainer(
        curriculum,
        reader=DatasetReader if args.sync else AsyncDatasetReader,
        tmpdir=args.temporary_directory,
        shuffle=args.shuffle,
    )

    state_tracker = StateTracker(args.state or f"{config_path}.state", restore=not args.do_not_resume)

    # Make trainer listen to `kill -SIGUSR1 $PID` to print dataset progress
    signal.signal(signal.SIGUSR1, lambda *, _: print_state(trainer.state(), sys.stderr))

    model_trainer = subprocess.Popen(
        args.trainer or config["trainer"],
        stdin=subprocess.PIPE,
        encoding="utf-8",
        preexec_fn=ignore_sigint,  # noqa: PLW1509
    )  # ignore_sigint makes marian ignore Ctrl-C. We'll stop it from here.

    assert model_trainer.stdin is not None

    # TODO: This logic looks complicated, should be able to do this simpler. Three scenarios:
    #   1. ctrl-c is pressed and trainer is told this is the end of the training data
    #   2. ctrl-c is pressed and trainer has much training data in its buffers, ctrl-c needs to be
    #      pressed again to tell trainer to really terminate. Just closing its stdin and waiting for
    #      it to notice takes too long
    #   3. trainer decides it has read enough and will train no longer. This is the BrokenPipeError
    #      scenario. We don't need to deal with multiple levels of terminating the trainer because
    #      the trainer is already dead at this point.
    try:
        try:
            for batch in state_tracker.run(trainer):
                model_trainer.stdin.writelines(batch)
        except KeyboardInterrupt:
            logger.info("[Trainer] Ctrl-c pressed, stopping training")

        # Levels of waiting for the trainer. This is reached either because we ran out of batches
        # or because ctrl-c was pressed. Pressing ctrl-c more advances to next level of aggressiveness.
        for stage in ["exit", "terminate", "kill"]:
            try:
                if stage == "exit":
                    model_trainer.stdin.close()
                elif stage == "terminate":
                    model_trainer.terminate()
                else:
                    model_trainer.kill()

                logger.info("[Trainer] waiting for trainer to %s. Press ctrl-c to be more aggressive", stage)
                return model_trainer.wait()  # blocking
            except KeyboardInterrupt:  # noqa: PERF203
                continue
    except BrokenPipeError:
        # BrokenPipeError is thrown by writelines() or close() and indicates that the child trainer
        # process is no more. We can safely retrieve its return code and exit with that, it should
        # not block at this point.
        logger.info("[Trainer] trainer stopped reading input")
        return model_trainer.wait()
    return 0


if __name__ == "__main__":
    sys.exit(main(parse_args()))
