# Kafka producer that reads the input data in a loop in order to simulate real time events
import os
import sys
from kafka import KafkaClient, KeyedProducer, SimpleConsumer
from datetime import datetime
import time

kafka = KafkaClient("54.67.126.144:9092")
source_file = '/home/ec2-user/000000_0'

def genData(topic):
    producer = KeyedProducer(kafka)
    while True:
        with open(source_file) as f:
	    count = 0
            for line in f:
               # print "SENDING LINE: " + str(count) + " : " + line 
                key = line.split(" ")[0]
                producer.send(topic, key, line.rstrip()) 
	        time.sleep(0.1)  # Creating some delay to allow proper rendering of the cab locations on the map
                count = count + 1
        source_file.close()

genData("CabData")
