# hello-kafka-streams

## Kafka Stack

### Apache Kafka

+ https://www.apache.org/dyn/closer.cgi?path=/kafka/2.7.0/kafka_2.13-2.7.0.tgz

Start Zookeeper
```shell
bin/zookeeper-server-start.sh config/zookeeper.properties
```
Start kafka
```shell
bin/kafka-server-start.sh config/server.properties
```  

### Confluent Inc

+ https://github.com/confluentinc/cp-all-in-one
+ https://docs.confluent.io/platform/current/tutorials/build-your-own-demos.html#cp-all-in-one

```shell
docker-compose up -d
```

[Control Center](http://localhost:9021)

### Landoop Lenses Box

+ https://hub.docker.com/r/landoop/fast-data-dev
+ https://github.com/lensesio/fast-data-dev

```shell
docker run -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 \
        -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=localhost \
        landoop/fast-data-dev:latest
```

[Lenses UI](http://localhost:3030)

