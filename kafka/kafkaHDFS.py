import os
import sys
from kafka import KafkaClient, KeyedProducer, SimpleConsumer
from datetime import datetime
#from docopt import docopt

kafka = KafkaClient("localhost:9092")
source_file = '/home/ubuntu/sortedData.txt'
tempfile_path = None
tempfile = None
batch_counter = 0
timestamp = None
"""
def genData(topic):
    producer = KeyedProducer(kafka)
    with open(source_file) as f:
        for line in f:
            print "sending line"
            key = line.split(" ")[0]
            producer.send(topic, key, line.rstrip())
    #producer.stop()
# def get_topics(zookeeper_hosts, topic_regex):
#     Uses shell zookeeper-client to read Kafka topics matching topic_regex from ZooKeeper."""
#     command        = "/usr/bin/zookeeper-client -server %s ls /brokers/topics | tail -n 1 | tr '[],' '   '" % ','.join(zookeeper_hosts)
#     topics         = os.popen(command).read().strip().split()
#     matched_topics = [ topic for topic in topics if re.match(topic_regex, topic) ]
#     return matched_topics

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

def flush_to_hdfs(output_dir, topic):
    global tempfile_path, tempfile, batch_counter
    tempfile.close()
    hadoop_dir = "%s/%s" % (output_dir, topic)
    hadoop_path = hadoop_dir + "/%s_%s.txt" % (timestamp, batch_counter)
    print "/usr/bin/hdfs dfs -mkdir %s" % hadoop_dir
    os.system("/usr/bin/hdfs dfs -mkdir %s" % hadoop_dir)
    print "/usr/bin/hdfs dfs -put -f %s %s" % (tempfile_path, hadoop_path)
    os.system("/usr/bin/hdfs dfs -put -f %s %s" % (tempfile_path, hadoop_path))
    os.remove(tempfile_path)
    batch_counter += 1
    tempfile_path = "/tmp/kafka_%s_%s_%s_%s.txt" % (topic, group, timestamp, batch_counter)
    tempfile = open(tempfile_path,"w")

def consume_topic(topic, group, output_dir, frequency):
    global timestamp, tempfile_path, tempfile
    print "Consumer Loading topic '%s' in consumer group %s into %s..." % (topic, group, output_dir)
    #get timestamp
    timestamp = standardized_timestamp(frequency)
    kafka_consumer = SimpleConsumer(kafka, group, topic, max_buffer_size=1310720000)
    
    #open file for writing
    tempfile_path = "/tmp/kafka_%s_%s_%s_%s.txt" % (topic, group, timestamp, batch_counter)
    tempfile = open(tempfile_path,"w")
    log_has_at_least_one = False #did we log at least one entry?
    while True:
        messages = kafka_consumer.get_messages(count=100, block=False) #get 5000 messages at a time, non blocking
        if not messages:
            print "no messages to read"
            continue
        for message in messages: #OffsetAndMessage(offset=43, message=Message(magic=0, attributes=0, key=None, value='some message'))
            log_has_at_least_one = True
            #print(message.message.value)
            tempfile.write(message.message.value + "\n")
        if tempfile.tell() > 10000000: #file size > 10MB
            flush_to_hdfs(output_dir, topic)
        kafka_consumer.commit() #save position in the kafka queue
    #exit loop
    if log_has_at_least_one:
        flush_to_hdfs(output_dir, topic)
    kafka_consumer.commit() #save position in the kafka queue
    return 0

if __name__ == '__main__':
    group = "batchStore"
    output = "/user/ubuntu/data"
    topic = "CabData"
    frequency = "1"

    print "\nConsuming topic: [%s] into HDFS" % topic
    #genData(topic)
    consume_topic(topic, group, output, frequency)
    #kafka.close()
    #sys.exit(0)
