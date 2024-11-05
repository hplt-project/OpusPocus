#!/usr/bin/env python3
import os
import signal
import subprocess
import sys
import time

import psutil

FILE = __file__.split("/")[-1]
MAX_ARGS = 3


def raise_nonzero_error(arguments, pid):  # noqa: ANN001, ANN201 fixit
    raise subprocess.SubprocessError(f"Process {pid} exited with a non-zero value. Wrapper arguments: {arguments}\n")  # noqa: EM102, TRY003


def main(argv):  # noqa: ANN001, ANN201 fixit
    """Wrapper for executing Bash commands that have process dependencies."""
    print(  # noqa: T201
        f"bash_runner_submit.py: Script pid: {os.getpid()}", file=sys.stderr
    )
    cmd_path = argv[1]
    proc_ids = []
    if len(argv) == MAX_ARGS and argv[2]:
        proc_ids = [int(proc) for proc in argv[2].split(" ")]

    procs = []
    for pid in proc_ids:
        try:
            procs.append(psutil.Process(pid))
        except psutil.NoSuchProcess:  # noqa: PERF203
            print("Process {} does not exist. Ignoring...", file=sys.stderr)  # noqa: T201

    gone, alive = psutil.wait_procs(procs)
    print(  # noqa: T201
        "bash_runner_submit.py: Running command: {}".format(" ".join(argv[1:])),
        file=sys.stderr,
    )
    for proc in gone:
        if proc.returncode is not None and proc.returncode > 0:
            raise_nonzero_error(argv[1:], proc.pid)

    proc = subprocess.Popen([cmd_path], shell=False)

    def propagate_signal(signum, _) -> None:  # noqa: ANN001
        print(f"{FILE} Received signal {signum}. Terminating child process...", file=sys.stderr)  # noqa: T201
        proc.send_signal(signum)
        proc.wait()
        if signum == signal.SIGUSR1:
            sys.exit(0)
        sys.exit(signum)

    # propagate caught signals and exit
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGUSR1):
        signal.signal(sig, propagate_signal)

    while proc.poll() is None:
        time.sleep(0.5)
    if proc.returncode:
        raise_nonzero_error(argv[1:], proc.pid)


if __name__ == "__main__":
    main(sys.argv)
    sys.exit(0)
