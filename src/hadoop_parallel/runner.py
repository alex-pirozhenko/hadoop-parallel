import tempfile
from functools import partial
from joblib import Parallel, delayed
import logging
import sys
from _mrjob import MRRunner
import os

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('HadoopParallel')


RUNNER_LOCAL = 'local'
RUNNER_HADOOP = 'hadoop'


def _indexed_call(idx, call):
    return idx, call()


class HadoopParallel(object):
    def __init__(self, n_cores=1, n_jobs=10, files=None, tmp_dir=None, runner=RUNNER_HADOOP):
        """
        Create HadoopParallel runner
        :param num_jobs: MR job-level parallelism (number of worker nodes)
        :param n_cores: MR task-level parallelism (number of threads per worker)
        :param files: List of files that should be distributed to MR workers
        """
        self.n_cores = n_cores
        self.n_jobs = n_jobs
        self.files = files if files else []
        self.hdfs_wd = tmp_dir if tmp_dir else tempfile.mktemp()
        self.local_wd = self.hdfs_wd
        self.local_input = self.hdfs_wd + '/input'
        self.runner = runner

    def __call__(self, delayed_calls):
        log.debug("Preparing callables...")
        prepared_calls = self._prepare_calls(delayed_calls)
        log.debug('Dumping callables..')
        try:
            os.makedirs(self.local_wd)
            with open(self.local_input, 'w') as out:
                protocol = MRRunner.INPUT_PROTOCOL()
                for k, v in enumerate(prepared_calls):
                    print >>out, protocol.write(k % self.n_jobs, v)

            log.debug("Preparing to run...")
            job = MRRunner(args=[
                self.local_input,
                '-r', self.runner,
                '--jobconf', 'mapred.reduce.tasks=%s' % self.n_jobs,
                '--hdfs-scratch-dir', '/tmp',
                '--interpreter', sys.executable,
            ])
            with job.make_runner() as runner:
                log.debug("Running job...")
                runner.run()
                log.debug("Collecting results...")
                result = sorted(((k, v) for k, v in map(job.parse_output_line, runner.stream_output())), key=lambda _: _[0])
                log.debug("Cleaning up")
                return [_[1] for _ in result]
        finally:
            os.remove(self.local_input)
            os.removedirs(self.local_wd)

    def _prepare_calls(self, calls):
        workers = [[] for _ in range(self.n_jobs)]
        i = 0
        for c in calls:
            workers[i % self.n_jobs].append(
                delayed(_indexed_call)(idx=i, call=c)
            )
            i += 1
        return [
            partial(Parallel(n_jobs=self.n_cores), worker)
            for worker in workers
        ]

