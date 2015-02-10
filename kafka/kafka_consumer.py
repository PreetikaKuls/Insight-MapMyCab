# This program implements a kafka consumer that stores a data stream to HDFS. It is subscribed to one topic (all cab data). 
import os
import sys
from kafka import KafkaClient, SimpleConsumer
from datetime import datetime

kafka = KafkaClient("localhost:9092")
source_file = '/home/ubuntu/data/cabDatabase.txt'
tempfile_path = None
tempfile = None
batch_counter = 0
timestamp = None

# Function to return timestamp in order to generate timestamped names for storage in HDFS
def standardized_timestamp(frequency, dt=None):
    if dt is None:
      dt = datetime.now()

    frequency = int(frequency)
    # Special case were frequency=0 so we only return the date component
    if frequency == 0:
        return dt.strftime('%Y-%m-%d')

    blocks = 60 / frequency
    standardized_minutes = {}
    for block in xrange(blocks):
        standardized_minutes[block] = block * frequency

    collapsed_minutes = (dt.minute / frequency)
    minutes = standardized_minutes.get(collapsed_minutes, 0)
    timestamp = datetime(dt.year, dt.month, dt.day, dt.hour, minutes, 0)

    return timestamp.strftime('%Y%m%d%H%M%S')

# Function to store data into HDFS 
def flush_to_hdfs(output_dir, topic):
    global tempfile_path, tempfile, batch_counter
   tempfile.close()
    hadoop_dir = "%s/%s" % (output_dir, topic)
    hadoop_path = hadoop_dir + "/%s_%s.txt" % (timestamp, batch_counter)
    print "/usr/bin/hdfs dfs -mkdir %s" % hadoop_dir
    os.system("/usr/bin/hdfs dfs -mkdir %s" % hadoop_dir)     # Issue command to create a directory
    print "/usr/bin/hdfs dfs -put -f %s %s" % (tempfile_path, hadoop_path)
    os.system("/usr/bin/hdfs dfs -put -f %s %s" % (tempfile_path, hadoop_path))  # Store newly generated file
    os.remove(tempfile_path)
    batch_counter += 1
    tempfile_path = "/tmp/kafka_%s_%s_%s_%s.txt" % (topic, group, timestamp, batch_counter) # identify file by the group, topic and timestamp
    tempfile = open(tempfile_path,"w")

# Function for storing data from the producer into a temporary file (which is later stored to HDFS through another function call) 
def consume_topic(topic, group, output_dir, frequency):
    global timestamp, tempfile_path, tempfile
    print "Consumer Loading topic '%s' in consumer group %s into %s..." % (topic, group, output_dir)
    timestamp = standardized_timestamp(frequency)
    kafka_consumer = SimpleConsumer(kafka, group, topic, max_buffer_size=1310720000)

    #open file for writing
    tempfile_path = "/tmp/kafka_%s_%s_%s_%s.txt" % (topic, group, timestamp, batch_counter)
    tempfile = open(tempfile_path,"w")
    log_has_at_least_one = False #did we log at least one entry?
    while True:
        messages = kafka_consumer.get_messages(count=1000, block=False) #get 1000 messages at a time, non blocking
        if not messages:
            print "no messages to read"
            continue   # If no messages are received, wait until there are more
        for message in messages:
            log_has_at_least_one = True
            #print(message.message.value)
            tempfile.write(message.message.value + "\n")
        if tempfile.tell() > 10000000: #file size > 10MB
            flush_to_hdfs(output_dir, topic)
        kafka_consumer.commit() # inform zookeeper of position in the kafka queue
    # 

if __name__ == '__main__':
    group = "batchStore"
    output = "/user/ubuntu/data"
    topic = "CabData"
    frequency = "1"

    print "\nConsuming topic: [%s] into HDFS" % topic
    consume_topic(topic, group, output, frequency)
                                                     
