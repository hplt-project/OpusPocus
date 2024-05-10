#!/usr/bin/env python3
import psutil
import subprocess
import sys


def main(argv):
    cmd_path = argv[1]
    procs = []
    try:
        for dep in argv[2].split(' '):
            procs.append(psutil.Process(int(dep)))
    except psutil.NoSuchProcess:
        print('Process {} does not exist. Ignoring', file=sys.stderr)

    gone, alive = psutil.wait_procs(procs)
    import pdb; pdb.set_trace()

    cmd = ['bash', cmd_path]
    if len(argv) > 3
        cmd += [argv[3]]
    proc = subprocess.Popen(cmd, shell=False)
    proc.communicate()


if __name__ == '__main__':
    main(sys.argv)
