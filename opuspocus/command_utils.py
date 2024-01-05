import logging
import subprocess
from pathlib import Path


BASH_WRAPPER = Path('scripts', 'bash_with_deps.sh')

logger = logging.getLogger(__name__)


def build_subprocess(cmd_path, args, jid_deps):
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
    cmd = ['bash', str(BASH_WRAPPER), ' '.join(jid_deps), str(cmd_path)]
    return cmd


def build_sbatch(cmd_path, args, jid_deps):
    cmd = ['sbatch', '--parsable']

    if jid_deps:
        cmd.append('--dependency')
        cmd.append(','.join(['afterok:{}'.format(jid) for jid in jid_deps]))

    runner_opts = args.runner_opts
    if runner_opts is not None:
        cmd += runner_opts.split(' ')

    cmd.append(str(cmd_path))
    return cmd


def build_sbatch_hq(cmd_path, args, jid_deps):
    # TODO: needs to be finished/tested on machines that support this
    # see: https://docs.csc.fi/apps/hyperqueue/
    """Sbatch hyperqueue support for LUMI HPC."""
    cmd = ['sbatch_hq', '--parsable']

    if jid_deps:
        cmd.append('--dependency')
        cmd.append(','.join(['afterok:{}'.format(jid) for jid in jid_deps]))

    runner_opts = args.runner_opts
    if runner_opts is not None:
        cmd += runner_opts.split(' ')

    cmd.append(str(cmd_path))
    return cmd


def build_hq_sbatch(cmd_path, args, jid_deps):
    # Init hq server if not initialized
    # TODO: finish this
    subprocess.Popen(['bash', 'scripts/launch_hq_server.sh'])
    subprocess.Popen([
        'bash',
        'scripts/launch_hq_workers.sh',
        'slurm',
        args.hq_worker_cpus,
        args.hq_worker_gpus,
        args.hq_worker_time_limit
    ])

    # Create sbatch command for hq
    cmd = ['./hq', 'submit', '--stdout=none', '--stderr=none']

    if jid_deps:
        cmd.append('--dependency')
        cmd.append(','.join(['afterok:{}'.format(jid) for jid in jid_deps]))

    runner_opts = args.runner_opts
    if runner_opts is not None:
        cmd += runner_opts.split(' ')

    cmd.append(str(cmd_path))
    return cmd
