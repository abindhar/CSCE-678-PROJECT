**CSCE-678  : TWITTER DATA ANALYSIS USING APACHE SPARK AND KAFKA**


The project goal is to perform the sentiment analysis on trending hashtags' tweets of a live sream


**Project flow:**\
	->Get the live stream of tweets(using twitter API and tweepy module) onto Kafka Producer\
	->On the producer, filter the live stream based on selective topics\
	->On the spark-installed consumer,\
		->Filter the tweets with hashtags\
		->Based on map reduce operations, obtain the top 10 trending hashtags\
		->Perform a sentiment analysis on all the tweets pertaining to trending hashtags\
		->Aggregate the analysis of all tweets for each ofthe trending hashtags and report the overall sentiment of hashtags
		
		

**Project Execution:**\
	->Create Twitter API account and get keys for fetching live stream of tweets\
	->Setup a kafka cluster with 3 brokers(producer on one broker and consumer on different one) and one Zookeeepr node\
	->Install spark on consumer node \
	->Start the zookeeper node : $bin/zkServer.sh start\
	->Start all the kafka nodes : $kafka-server-start.sh config/server.properties\
	->Start the producer : $python3 producer.py \
	->Start the consumer by Spark submit:- spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.2.jar,spark-core_2.11-1.5.2.logging.jar consumer.py\
	->Trending hashtags with the overallsentiment analysis  will be displayed on the conumer console
	
	
**Final source files:** \
Please check the below source files for the final working implementation : 

**Demo :** \
A complete demo of the project is available in the link : https://drive.google.com/open?id=17epWAJ_lpYV8rgE6l-JEbMr3S3e0QCML



