#!/usr/bin/env python3
import signal
import subprocess
import sys

import psutil

FILE = __file__.split("/")[-1]


def raise_nonzero_error(arguments, pid):  # noqa: ANN001, ANN201 fixit
    raise subprocess.SubprocessError(f"Process {pid} exited with a non-zero value. Wrapper arguments: {arguments}\n")  # noqa: EM102, TRY003


def main(argv):  # noqa: ANN001, ANN201 fixit
    """Wrapper for executing Bash commands that have process dependencies."""
    cmd_path = argv[1]
    proc_ids = []
    if argv[2]:
        proc_ids = [int(proc) for proc in argv[2].split(" ")]

    procs = []
    for pid in proc_ids:
        try:
            procs.append(psutil.Process(pid))
        except psutil.NoSuchProcess:  # noqa: PERF203
            print("Process {} does not exist. Ignoring...", file=sys.stderr)  # noqa: T201

    gone, alive = psutil.wait_procs(procs)
    print(  # noqa: T201
        "bash_runner_submit.py: Running command: {}".format(" ".join(argv)),
        file=sys.stderr,
    )
    for proc in gone:
        if proc.returncode is not None and proc.returncode > 0:
            raise_nonzero_error(argv[1:], proc.pid)

    proc = subprocess.Popen([cmd_path], shell=False)

    def propagate_signal(signum, _) -> None:  # noqa: ANN001
        print(f"{FILE} Received signal {signum}. Terminating child process...", file=sys.stderr)  # noqa: T201
        proc.send_signal(signum)
        sys.exit(signum)

    # propagate caught signals and exit
    for sig in set(signal.Signals):
        try:
            signal.signal(sig, propagate_signal)
        except (ValueError, OSError, RuntimeError):  # noqa: PERF203
            print(f"{FILE} Skipping signal {sig}", file=sys.stderr)  # noqa: T201

    rc = proc.wait()
    if rc:
        raise_nonzero_error(argv[1:], proc.pid)


if __name__ == "__main__":
    main(sys.argv)
