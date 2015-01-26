import os
import sys
from kafka import KafkaClient, KeyedProducer, SimpleConsumer
from datetime import datetime

kafka = KafkaClient("localhost:9092")
source_file = '/home/ubuntu/data/cabDatabase.txt'
tempfile_path = None
tempfile = None
batch_counter = 0
timestamp = None

def genData(topic):
    producer = KeyedProducer(kafka)
    with open(source_file) as f:
        for line in f:
            #print "sending line"
            key = line.split(" ")[0]
            producer.send(topic, key, line.rstrip())
    #producer.stop()

genData("CabData")
"""
kafka = KafkaClient("localhost:9092")
destination = '/home/ubuntu/kafka_proc/outputLog.csv'

#Generate producer
def genData():
    producer = KeyedProducer(kafka)
    with open('/home/ubuntu/kafka_proc/testInput.txt') as f:
        for line in f:
            key, message = line.split(",")
            message = key + ', ' + message
            producer.send("one-topic", key, message)

"""
