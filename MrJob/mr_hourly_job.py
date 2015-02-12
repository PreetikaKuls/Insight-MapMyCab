# This function utilizes the MrJob tool to perform MapReduce operations and generate additional 
# metrics for the cab data: Miles travelled.
#!/usr/bin/python
import mrjob
from mrjob.job import MRJob
import datetime
import geopy
from geopy.distance import great_circle # Calculate distance from latitude and longitude data

class HourlyJob(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol # format the output so that the reducer outputs key, value separated by '\t'
    def mapper(self, _, line):
	fields = line.split(" ")
	if (len(fields) != 5):                # ensure that line has all the fields
        	uid, lat, lng, occ, timestamp = fields
        	timestamp = int(line.split(" ")[4])
        	yield uid, [timestamp, lat, lng, int(occ)] # combine values into a list, to collect metrics for each key (cab) 
    
    def reducer(self, key, values):
        final_info = []
        prev_occ, prev_lat, prev_long = 0, 0.0, 0.0
        values = list(values) # convert data to list type
        values.sort(key = lambda row: row[0])  # mapper does not guarantee preservation of order for each key after the map stage
        for i, item in enumerate(values):
            cur_lat, cur_long = float(item[1]), float(item[2])
            cur_occ = item[3]
            if i != 0:                     # if it is the beginning of trajectory, distance = 0 miles
                dist = great_circle((cur_lat, cur_long), (prev_lat, prev_long)).miles
            else: dist = 0
            prev_lat, prev_long = cur_lat, cur_long
            prev_occ = cur_occ
            occ_list = [str(item[0]), str(cur_occ), str(cur_lat), str(cur_long), str(dist)]
            cab_info = '\t'.join(occ_list) # convert to tab separated string  
            yield key, cab_info

if __name__ == '__main__':
    HourlyJob.run()
