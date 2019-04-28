from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys
import os

if __name__ == "__main__":

        #Create Spark Context to Connect Spark Cluster
    #os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-streaming-kafka-assembly_2.10-1.6.0.jar pyspark-shell'
    sc = SparkContext(appName="TweetCount")
    #sc.setLogLevel("DEBUG")
        #Set the Batch Interval is 10 sec of Streaming Context
    ssc = StreamingContext(sc, 60)
    topics = ["twitterstream"]
    params = {"bootstrap.servers":"localhost:9092"}
        #Create Kafka Stream to Consume Data Comes From Twitter Topic
        #localhost:2181 = Default Zookeeper Consumer Address
    kafkaStream = KafkaUtils.createDirectStream(ssc,topics,params)
    #print('td',kafkaStream)
    #kafkaStream = KafkaUtils.createStream(ssc, '10.128.0.16:2181','id', {'twitterstream':1})
    
    #parsed = kafkaStream.map(lambda v: json.loads(v[1]))
    parsed = kafkaStream.map(lambda v: v[1])
    parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
    #Count the number of tweets per User
    user_counts = parsed.map(lambda tweet: (tweet['user']["screen_name"], 1)).reduceByKey(lambda x,y: x + y)
    #print('This is Jarvis') 
        #Print the User tweet counts
    user_counts.pprint()
        #Start Execution of Streams
    #kafkaStream = KafkaUtils.createStream(ssc, '10.128.0.16:2181','id', {'raja':1})
    #kafkaStream.pprint()
    #lines = kafkaStream.map(lambda x: x[1])
    #lines.pprint()
    ssc.start()
    ssc.awaitTermination()
