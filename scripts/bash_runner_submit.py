#!/usr/bin/env python3
import psutil
import subprocess
import sys


def raise_nonzero_error(arguments, pid):
    raise subprocess.SubprocessError(
        "Process {} exited with a non-zero value. Wrapper arguments: {}\n".format(pid, arguments)
    )


def main(argv):
    """Wrapper for executing Bash commands that have process dependencies."""
    cmd_path = argv[1]
    proc_ids = []
    if argv[2]:
        proc_ids = [int(proc) for proc in argv[2].split(" ")]

    procs = []
    for pid in proc_ids:
        try:
            procs.append(psutil.Process(pid))
        except psutil.NoSuchProcess:
            print("Process {} does not exist. Ignoring...", file=sys.stderr)

    gone, alive = psutil.wait_procs(procs)
    print(
        "bash_runner_submit.py: Running command: {}".format(" ".join(argv)),
        file=sys.stderr,
    )
    for proc in gone:
        if proc.returncode is not None and proc.returncode > 0:
            raise_nonzero_error(argv[1:], proc.pid)

    proc = subprocess.Popen([cmd_path], shell=False)
    rc = proc.wait()
    if rc:
        raise_nonzero_error(argv[1:], proc.pid)


if __name__ == "__main__":
    main(sys.argv)
