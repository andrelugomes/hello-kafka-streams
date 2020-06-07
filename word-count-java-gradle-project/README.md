# Word Count Java Gradle Project

## Set up

+ Kafka 
+ Zookeeper
+ topics
+ Build Project

```shell script
bin/zookeeper-server-start.sh config/zookeeper.properties
```

```shell script
bin/kafka-server-start.sh config/server.properties
```  

```shell script
bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic streams-plaintext-input
```  

```shell script
bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic streams-wordcount-output \
    --config cleanup.policy=compact
```  

### Consumer
```shell script
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic streams-wordcount-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```  

### Word Count Stream APP

Build

```shell script
./gradlew build
```

Run

```shell script
java -jar build/libs/word-count-stream.jar
```