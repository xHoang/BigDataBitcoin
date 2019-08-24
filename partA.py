from mrjob.job import MRJob
import datetime

class partA(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if (len(fields)==5):
                month = datetime.datetime.fromtimestamp(int(fields[2])).strftime("%Y-%m")
                yield(month,1)
        except:
            pass

    def reducer(self, month, counts):
        total = sum(counts)
        yield(month, total)


    def combiner(self, month, counts):
        total = sum(counts)
        yield(month, total)

if __name__ == '__main__':
    partA.run()
