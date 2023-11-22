# TODO: create command (class?) registration, move each builder class to a
# separate file
import subprocess
import logging


logger = logging.getLogger(__name__)


def build_subprocess(cmd_path, args, jid_deps):
    if args.runner == 'sbatch':
        cmd = build_sbatch(cmd_path, args, jid_deps)
    else:
        raise NotImplementedError()
    sub = {
        'process': subprocess.Popen(cmd),
        'jobid' : None,
    }
    if args.runner == 'sbatch':
        sub['jobid'] = sub['process'].stdout

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
