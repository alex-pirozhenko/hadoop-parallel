import unittest
from hadoop_parallel import HadoopParallel, RUNNER_LOCAL
from functools import partial;


class TestRunner(unittest.TestCase):
    def setUp(self):
        pass

    def test_local(self):
        res = HadoopParallel(runner=RUNNER_LOCAL)(
            partial(sum, range(_)) for _ in range(10)
        )
        self.assertEquals(
            [partial(sum, range(_))() for _ in range(10)],
            res
        )