#!/usr/bin/python
from mrjob.job import MRJob
import datetime
import geopy
from geopy.distance import great_circle

class HourlyJob(MRJob):
    def mapper(self, _, line):
        uid, lat, lng, occ, timestamp = line.split(" ")
        #final_tuple = (uid, timestamp, float(lat), float(lng), int(occ))
        #line = line.rstrip('\n')
        timestamp = int(line.split(" ")[4])
        #yield timestamp - timestamp % (60*60*24), int(occ)
        yield uid, [timestamp, lat, lng, int(occ)]
    #def combiner():

    def reducer(self, key, values):
        final_info = []
        prev_occ, prev_lat, prev_long = 0, 0.0, 0.0
        i = 0
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
            #geolocator = Nominatim()
            #location = geolocator.reverse(str(cur_lat) + " , " + str(cur_long))
            #zipcode = location.raw['address']['postcode']
            i = i + 1
            #  time = datetime.datetime.fromtimestamp(float(item[0]))
            occ_list = [str(item[0]), str(cur_occ), str(pickup), str(dropoff), str(cur_lat), str(cur_long), str(dist)]      
            cab_info = ', '.join(occ_list)
            final_info.append(cab_info)
        #yield key.strftime("%Y-%m-%d %H:%M:%S"), occ_list 
        yield key, final_info

if __name__ == '__main__':
    HourlyJob.run()
