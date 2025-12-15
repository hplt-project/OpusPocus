#!/usr/bin/env python3
import importlib
import logging
import os
import signal
import subprocess
import sys
from typing import List, Sequence

import psutil

FILE = __file__.split("/")[-1]
MAX_ARGS = 3
MODULE_NAME = "command"

logger = logging.getLogger(__name__)


def raise_nonzero_error(arguments: List[str], pid: int) -> None:
    err_msg = f"Dependency pid={pid} exited with a non-zero value. Current wrapper arugments: {arguments}"
    raise subprocess.SubprocessError(err_msg)


def main(argv: Sequence[str]) -> int:
    """Wrapper for executing Bash commands that have process dependencies."""
    logger.debug("[%s] Script pid: %s", FILE, os.getpid())
    cmd_path = argv[1]
    proc_ids = []
    if len(argv) == MAX_ARGS and argv[2]:
        proc_ids = [int(proc) for proc in argv[2].split(" ")]

    procs = []
    for pid in proc_ids:
        try:
            procs.append(psutil.Process(pid))
        except psutil.NoSuchProcess:  # noqa: PERF203
            logger.warning("[%s] Process %s does not exist. Ignoring...", FILE, pid)

    gone, alive = psutil.wait_procs(procs)
    logger.info(
        "[%s] Executing command.main() (module: %s) method with the following arguments: %s", FILE, argv[1], argv[2]
    )
    for proc in gone:
        if proc.returncode is not None and proc.returncode > 0:
            raise_nonzero_error(argv[1:], proc.pid)

    def propagate_signal(signum, _) -> None:  # noqa: ANN001
        logger.info("[%s] Received signal %s. Terminating child process...", FILE, signum)
        proc.send_signal(signum)
        proc.wait()
        if signum == signal.SIGUSR1:
            return 0
        return signum

    # propagate caught signals and exit
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGUSR1):
        signal.signal(sig, propagate_signal)

    # Import the scripts main method and execute the script
    spec = importlib.util.spec_from_file_location(MODULE_NAME, cmd_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    module.main([cmd_path])

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
