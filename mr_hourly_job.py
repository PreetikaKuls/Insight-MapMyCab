#!/usr/bin/python
import mrjob
from mrjob.job import MRJob
import datetime
import geopy
from geopy.distance import great_circle

class HourlyJob(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol
    def mapper(self, _, line):
	lineparts = line.split(" ")
	if (len(lineparts) == 5): 
        	uid, lat, lng, occ, timestamp = lineparts
        	timestamp = int(line.split(" ")[4])
        	yield uid, [timestamp, lat, lng, int(occ)]

    def reducer(self, key, values):
        final_info = []
        prev_occ, prev_lat, prev_long = 0, 0.0, 0.0
        i = 0
        values = list(values)
        values.sort(key = lambda row: row[0])
        for item in values:
            cur_lat, cur_long = float(item[1]), float(item[2])
            cur_occ = item[3]
            pickup, dropoff = 0, 0 
            if cur_occ - prev_occ == 1:
                pickup = 1
            if prev_occ - cur_occ == 1:
                dropoff = 1
            if i != 0:
                dist = great_circle((cur_lat, cur_long), (prev_lat, prev_long)).miles
            else: dist = 0
            prev_lat, prev_long = cur_lat, cur_long
            prev_occ = cur_occ
            i = i + 1
            occ_list = [str(item[0]), str(cur_occ), str(pickup), str(dropoff), str(cur_lat), str(cur_long), str(dist)]
            cab_info = '\t'.join(occ_list) 
            yield key, cab_info

if __name__ == '__main__':
    HourlyJob.run()
