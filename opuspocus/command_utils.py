import logging
import subprocess
from pathlib import Path


BASH_WRAPPER = Path('scripts', 'bash_with_deps.sh')

logger = logging.getLogger(__name__)


def build_subprocess(cmd_path, args, jid_deps):
    """Build a function for step execution specified in `args`.

    Args:
        cmd_path: path to the step.command
        args: OpusPocus arguments
        jid_deps: list of IDs (pid, job_id) of the executed step prerequisities

    Returns:
        `subprocess` dict containing the `process` object and its `jobid`
    """
    if args.runner == 'bash':
        logger.warn(
            '{} is currently experimental. Use at your own risk.'
            ''.format(args.runner)
        )
        cmd = build_bash(cmd_path, args, jid_deps)
    elif args.runner == 'sbatch':
        cmd = build_sbatch(cmd_path, args, jid_deps)
    elif args.runner == 'sbatch-hq':
        raise NotImplementedError(
            '{} is only a placeholder at the moment.'.format(args.runner)
        )
        cmd = build_sbatch_hq(cmd_path, args, jid_deps)
    elif args.runner == 'hq-sbatch':
        raise NotImplementedError(
            '{} is only a placeholder at the moment.'.format(args.runner)
        )
        cmd = build_hq_sbatch(cmd_path, args, jid_deps)
    else:
        raise NotImplementedError()

    logger.info('Executing command: "{}"'.format(' '.join(cmd)))
    sub = {
        'process': subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False
        ),
        'jobid' : None,
    }
    if args.runner == 'bash':
        sub['jobid'] = sub['process'].pid
    elif args.runner == 'sbatch' or args.runner == 'sbatch-hq':
        sub['jobid'] = int(sub['process'].stdout.readline())

    return sub


def build_bash(cmd_path, args, jid_deps):
    """(Experimental) Build a function for local Bash execution."""
    cmd = ['bash', str(BASH_WRAPPER), ' '.join(jid_deps), str(cmd_path)]
    return cmd


def build_sbatch(cmd_path, args, jid_deps):
    """Build a function for execution on SLURM using sbatch."""
    cmd = ['sbatch', '--parsable']

    if jid_deps:
        cmd.append('--dependency')
        cmd.append(','.join(['afterok:{}'.format(jid) for jid in jid_deps]))

    runner_opts = args.runner_opts
    if runner_opts is not None:
        cmd += runner_opts.split(' ')

    cmd.append(str(cmd_path))
    return cmd
