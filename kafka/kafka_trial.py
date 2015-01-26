from kafka import KafkaClient, KeyedProducer, SimpleConsumer
import csv
import os
kafka = KafkaClient("localhost:9092")
source = '/home/ubuntu/data/cabDatabase.txt'

#Generate producer
def genData():
    producer = KeyedProducer(kafka)
    with open(source) as f:
        for line in f:
            key = line.split(" ")[0]
            producer.send("CabTrajectory", key, line)
    #producer.stop()

def update_hdfs(inputFile):
    dest_dir = "/user/ubuntu/Newoutput"
    inputFile.close()
    print "reached inside updated_hdfs"
    #os.system("sudo su hdfs") #Change directory to HDFS
    os.system("/usr/bin/hdfs dfs -mkdir %s" % dest_dir)
    print "created directory"
    os.system("/usr/bin/hdfs dfs -copyFromLocal -f %s %s" % (source, dest_dir))
    #os.remove(inputFile)    

#Generate consumer
def consumeData():
    textFile = open(source, 'wb')
    consumer = SimpleConsumer(kafka, "batch_group", "CabTrajectory")
    messages = consumer.get_messages(count = 4, block=False)
    for message in messages:
        print(message.message.value)
        writeData = csv.writer(csvfile)
        writeData.writerow([message.message.value])
        print "message saved"
    #print csvfile.tell()
    #if csvfile.tell() > 0:
    print "file is large"
    update_hdfs(csvfile)
    consumer.commit()

genData()
consumeData()

kafka.close()


