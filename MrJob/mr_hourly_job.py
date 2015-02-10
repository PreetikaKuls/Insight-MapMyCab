# This function utilizes the MrJob tool to perform MapReduce operations and generate additional 
# metrics for the cab data. Pickups, Dropoffs, Miles travelled.
#!/usr/bin/python
import mrjob
from mrjob.job import MRJob
import datetime
import geopy
from geopy.distance import great_circle # Calculate distance from latitude and longitude data

class HourlyJob(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol # format the output so that the reducer outputs key, value separated by '\t'
    # Mapper formats the fields for each cab at a timestamp into a list
    def mapper(self, _, line):
	fields = line.split(" ")
	if (len(fields) != 5):                # ensure that line has all the fields
        	uid, lat, lng, occ, timestamp = fields
        	timestamp = int(line.split(" ")[4])
        	yield uid, [timestamp, lat, lng, int(occ)] # combine values into a list, to collect metrics for each key (cab) 
    # Reducer iterates over the timeseries for each cab (key) to calculate the pickups, dropoffs and intermediate distance for 
    # succesive records (trajectory)
    def reducer(self, key, values):
        final_info = []
        prev_occ, prev_lat, prev_long = 0, 0.0, 0.0
        values = list(values) # convert data to list type
        #values.sort(key = lambda row: row[0])  # mapper does not guarantee preservation of order for each key after the map stage
        for i, item in enumerate(values):
            cur_lat, cur_long = float(item[1]), float(item[2])
            cur_occ = item[3]
            pickup, dropoff = 0, 0 
            if cur_occ - prev_occ == 1:    # if occ flips from 0 to 1, record pickup
                pickup = 1
            if prev_occ - cur_occ == 1:    # if occ flips from 1 to 0, record dropoff
                dropoff = 1
            if i != 0:                     # if it is the beginning of trajectory, distance = 0 miles
                dist = great_circle((cur_lat, cur_long), (prev_lat, prev_long)).miles
            else: dist = 0
            dist = 0
            prev_lat, prev_long = cur_lat, cur_long
            prev_occ = cur_occ
            occ_list = [str(item[0]), str(cur_occ), str(pickup), str(dropoff), str(cur_lat), str(cur_long), str(dist)]
            cab_info = '\t'.join(occ_list) # convert to tab separated string  
            yield key, cab_info

if __name__ == '__main__':
    HourlyJob.run()
