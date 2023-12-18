# TODO: create command (class?) registration, move each builder class to a
# separate file
import subprocess
import logging


logger = logging.getLogger(__name__)


def build_subprocess(cmd_path, args, jid_deps):
    if args.runner == 'sbatch':
        cmd = build_sbatch(cmd_path, args, jid_deps)
    elif args.runner == 'sbatch-hq':
        cmd = build_sbatch_hq(cmd_path, args, jid_deps)
    else:
        raise NotImplementedError()
    sub = {
        'process': subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ),
        'jobid' : None,
    }
    if args.runner == 'sbatch' or args.runner == 'sbatch-hq':
        sub['jobid'] = int(sub['process'].stdout.readline())

    return sub


def build_sbatch(cmd_path, args, jid_deps):
    cmd = ['sbatch', '--parsable']

    if jid_deps:
        cmd.append('--dependency')
        cmd.append(','.join(['afterok:{}'.format(jid) for jid in jid_deps]))

    runner_opts = args.runner_opts
    if runner_opts is not None:
        cmd += runner_opts.split(' ')

    cmd.append(str(cmd_path))
    logger.info('Executing command: "{}"'.format(' '.join(cmd)))

    return cmd

def build_sbatch_hq(cmd_path, args, jid_deps):
    """Sbatch hyperqueue support for LUMI HPC."""
    cmd = ['sbatch_hq', '--parsable']

    if jid_deps:
        cmd.append('--dependency')
        cmd.append(','.join(['afterok:{}'.format(jid) for jid in jid_deps]))

    runner_opts = args.runner_opts
    if runner_opts is not None:
        cmd += runner_opts.split(' ')

    cmd.append(str(cmd_path))
    logger.info('Executing command: "{}"'.format(' '.join(cmd)))

    return cmd
