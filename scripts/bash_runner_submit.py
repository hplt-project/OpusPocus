#!/usr/bin/env python3
import psutil
import subprocess
import sys


def main(argv):
    """Wrapper for executing Bash commands that have process dependencies."""
    cmd_path = argv[1]
    proc_ids = []
    if argv[2]:
        proc_ids = [int(proc) for proc in argv[2].split(' ')]

    procs = []
    try:
        for pid in proc_ids:
            procs.append(psutil.Process(pid))
    except psutil.NoSuchProcess:
        print('Process {} does not exist. Ignoring', file=sys.stderr)

    gone, alive = psutil.wait_procs(procs)
    print(
        'bash_runner_submit.py: Running command: {}'.format(' '.join(argv)),
        file=sys.stderr
    )
    for proc in gone:
        if proc.returncode is not None and proc.returncode > 0:
            print('Process {} exited with non-zero value.'.format(proc))
            sys.exit(1)

    cmd = ['bash', cmd_path]
    if len(argv) > 3:
        cmd += [argv[3]]
    proc = subprocess.Popen(cmd, shell=False)
    proc.communicate()


if __name__ == '__main__':
    main(sys.argv)
