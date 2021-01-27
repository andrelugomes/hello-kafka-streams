```shell
./kafka-console-producer.sh --topic input --bootstrap-server localhost:9092 --property "parse.key=true" --property "key.separator=:"
```

```shell
./kafka-console-consumer.sh --bootstrap-server localhost:9092 \                          ─╯
    --topic output \
    --from-beginning \
    --property print.key=true \
    --property print.value=true
```

# JSON Serde

```shell
kafka-topics \
--create \
--bootstrap-server kafka:9092 \
--replication-factor 1 \
--partitions 1 \
--topic temperatures-topic

./kafka-topics.sh \                                                ─╯
--create \
--bootstrap-server localhost:9092 \
--replication-factor 1 \
--partitions 1 \
--topic high-temperatures-output


./kafka-console-producer.sh --topic temperature-input --bootstrap-server localhost:9092 --property "parse.key=true" --property "key.separator=:"

"S1":{"station":"S1", "temperature": 10.2, "timestamp": 1}
"S1":{"station":"S1", "temperature": 11.2, "timestamp": 2}
"S1":{"station":"S1", "temperature": 11.1, "timestamp": 3}
"S1":{"station":"S1", "temperature": 12.5, "timestamp": 4}
"S2":{"station":"S2", "temperature": 15.2, "timestamp": 1}
"S2":{"station":"S2", "temperature": 21.7, "timestamp": 2}
"S2":{"station":"S2", "temperature": 25.1, "timestamp": 3}
"S2":{"station":"S2", "temperature": 27.8, "timestamp": 4}


./kafka-console-consumer.sh --bootstrap-server localhost:9092 \    ─╯
    --topic high-temperature-output \
    --from-beginning \             
    --property print.key=true \
    --property print.value=true
```

## FlatMap

```shell
var lines = listOf("Kafka is a really cool technology",
      "Many enterprises use Kafka and Kafka Streams",
      "I want to hear more about Kafka Streams and KSQL",
      "KSQL is for Kafka what SQL is for databases")
  println(lines)
[Kafka is a really cool technology, Many enterprises use Kafka and Kafka Streams, I want to hear more about Kafka Streams and KSQL, KSQL is for Kafka what SQL is for databases]

var mapped = lines.map { x -> x.split(" ") }
    println(mapped)
[[Kafka, is, a, really, cool, technology], [Many, enterprises, use, Kafka, and, Kafka, Streams], [I, want, to, hear, more, about, Kafka, Streams, and, KSQL], [KSQL, is, for, Kafka, what, SQL, is, for, databases]]

var flatted = lines.flatMap { x -> x.split(" ") }
   println(flatted)
[Kafka, is, a, really, cool, technology, Many, enterprises, use, Kafka, and, Kafka, Streams, I, want, to, hear, more, about, Kafka, Streams, and, KSQL, KSQL, is, for, Kafka, what, SQL, is, for, databases]
```