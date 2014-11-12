import hadoopy
import cPickle
import os
__doc__ = """
This script manages execution on slave. This version implements a reducer-side code running.
"""


def _get_location():
    return os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class Mapper(object):
    def __init__(self):
        pass

    def map(self, key, value):
        yield key, value


class Reducer(object):
    def __init__(self):
        pass

    def reduce(self, key, values):
        for value in values:
            for res in cPickle.loads(value)():
                yield res[0], cPickle.dumps(res[1], protocol=2)


if __name__ == '__main__':
    hadoopy.run(Mapper, Reducer)