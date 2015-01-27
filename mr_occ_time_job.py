from mrjob.job import MRJob
import datetime

class OccTimeJob(MRJob):
    def mapper(self, _, line):
        line = line.split(" ")
        timeStamp = int(line[4])
        occ = int(line[3])
        yield timeStamp - timeStamp % (60*60*24), occ


    def reducer(self, key, values):
        key = datetime.datetime.fromtimestamp(float(key))
        yield key.strftime("%Y-%m-%d %H:%M:%S"), sum(values)

if __name__ == '__main__':
    OccTimeJob.run()
