# Define custom sink using Kafka

The basic work flow 

1. User as (Kafka Producer topic "sensor") 
2. Flink (Kafka Consumer topic: "sensor")
3. Serialize data  
4. Sink (Kafka Producer topic: "sinkTest")
5. Kafka Consumer (topic:"sinkTest")

Command to run this test

start zookeeper

`./bin/zookeeper-server-start.sh config/zookeeper.properties`

start kafka server

` ./bin/kafka-server-start.sh config/server.properties`

create kafka producer for topic "sensor"

`./bin/kafka-console-producer.sh --broker-list localhost:9092  --topic sensor`

create kafka consumer for topic "sinktest"

`.bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sinktest`

Run Flink application，under kafka producer console input data，check kafka producer console output result

**input**
```
sensor_1,1547718199,35.8
sensor_6,1547718201,15.4
```

**output**
```
SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
SensorReading{id='sensor_6', timestamp=1547718201, temperature=15.4}
```