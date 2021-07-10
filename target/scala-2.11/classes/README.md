
## Kafka

// Start the ZooKeeper service 
//Note: Soon, ZooKeeper will no longer be required by Apache Kafka.
$ bin/zookeeper-server-start.sh config/zookeeper.properties
Open another terminal session and run:

// Start the Kafka broker service
$ bin/kafka-server-start.sh config/server.properties

### make a new topic
$ bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092

### list all topics
bin/kafka-topics.sh --list --zookeeper localhost:2181

### desc specific topic
$ bin/kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092
Topic:quickstart-events  PartitionCount:1    ReplicationFactor:1 Configs:
Topic: quickstart-events Partition: 0    Leader: 0   Replicas: 0 Isr: 0

// STEP 4: WRITE SOME EVENTS INTO THE TOPIC

$ bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
This is my first event
This is my second event


//STEP 5: READ THE EVENTS
//Open another terminal session and run the console consumer client to read the events you just created:

$ bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
This is my first event
This is my second event

referer :
https://kafka.apache.org/quickstart#quickstart_send
