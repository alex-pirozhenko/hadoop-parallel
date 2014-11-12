import tempfile
import cPickle
import hadoopy
from functools import partial
from joblib import Parallel
import logging
from mapreduce import _get_location
logging.basicConfig(level=logging.DEBUG)


def _indexed_call(idx, call):
    return idx, call()


class HadoopParallel(object):
    def __init__(self, n_cores=1, n_jobs=10, files=None, tmp_dir=None):
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
        self.hdfs_input = self.hdfs_wd + '/input'
        self.hdfs_output = self.hdfs_wd + '/output'

    def __call__(self, delayed_calls):
        logging.debug("Preparing and pickling callables...")
        prepared_calls = self._prepare_calls(delayed_calls)
        logging.debug("Dumping callables to " + self.hdfs_input + "...")
        hadoopy.mkdir(self.hdfs_input)
        hadoopy.writetb(self.hdfs_input, enumerate(prepared_calls))
        logging.debug("Launching the job...")
        out = hadoopy.launch_frozen(
            in_name=self.hdfs_input,
            out_name=self.hdfs_output,
            script_path=_get_location() + 'mapreduce.py',
            files=self.files
        )
        return [cPickle.loads(_[1]) for _ in sorted(out['output'], key=lambda _: _[0])]

    def _prepare_calls(self, calls):
        workers = [[] for _ in range(self.n_jobs)]
        i = 0
        for c in calls:
            workers[i % self.n_jobs].append(partial(_indexed_call, idx=i, call=c))
            i += 1
        return [
            cPickle.dumps(partial(Parallel(n_jobs=self.n_cores), worker), protocol=2)
            for worker in workers
        ]

