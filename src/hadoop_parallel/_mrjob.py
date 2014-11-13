import mrjob.job
import mrjob.protocol
import logging
import sys

logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)


class MRRunner(mrjob.job.MRJob):
    INPUT_PROTOCOL = mrjob.protocol.PickleProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.PickleProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol

    def mapper(self, key, value):
        yield key, value

    def reducer(self, _, values):
        for call in values:
            for res in call():
                yield res[0], res[1]


if __name__ == '__main__':
    MRRunner().run()
