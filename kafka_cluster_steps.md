Working steps:

Zookeeper server :  after zoo.cfg is made run bin/zkServer.sh start

Kafka brokers:

vim config/server.properties 

bin/kafka-server-start.sh config/server.properties

bin/kafka-server-start.sh config/server.properties

bin/kafka-topics.sh --create --zookeeper 10.128.0.16:2181 --replication-factor 2 --partitions 3 --topic sample_test

bin/kafka-topics.sh --list --bootstrap-server localhost:9092

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sample_test

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sample_test --from-beginning
