import mrjob.job
import mrjob.protocol


class MRRunner(mrjob.job.MRJob):
    INPUT_PROTOCOL = mrjob.protocol.PickleProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.PickleProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol

    def reducer(self, _, values):
        for call in values:
            for res in call():
                yield res[0], res[1]


if __name__ == '__main__':
    MRRunner().run()
