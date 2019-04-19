Working steps:

Go to Zookeeper server and cd to zookeeper directory.  
Make sure zoo.cfg file is ready.
Now run command - bin/zkServer.sh start

Go to servers with Kafka brokers.
Make sure config/server.properties are updated

Run start command in all brokers: bin/kafka-server-start.sh config/server.properties

If topic not created, use this command to create one. bin/kafka-topics.sh --create --zookeeper 10.128.0.16:2181 --replication-factor 2 --partitions 3 --topic sample_test

Check if topic is created. bin/kafka-topics.sh --list --bootstrap-server localhost:9092

Run on producer. bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sample_test

Run on consumer. bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sample_test --from-beginning
