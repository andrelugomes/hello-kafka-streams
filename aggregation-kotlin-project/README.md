# Aggregation kafka Streams

## Kafka Stack

+ [Confluent Plataform](https://docs.confluent.io/current/quickstart/ce-docker-quickstart.html#ce-docker-quickstart)
+ [Confluent Docker Github](https://github.com/confluentinc/cp-all-in-on)

```shell script
$ docker-compose up 
```
## Produce Messages

+ Kafka Rest Proxy
+ [Base64](https://www.base64encode.org/)

## Keys

+ 1 => MQ==
+ 2 => Mg==
+ 3 => Mw==

## Values

+ 10 => MTA=
+ 20 => MjA=
+ 30 => MzA=

## Records
```json
"records": [
    {
        "key": "1",
        "value": "12"
    },
    {
        "key": "1",
        "value": "20"
    },
    {
        "key": "1",
        "value": "30"
    },
    {
        "key": "2",
        "value": "10"
    }
]
```
```json
"records": [
    {
        "key": "MQ==",
        "value": "MTA="
    },
    {
        "key": "MQ==",
        "value": "MjA="
    },
    {
        "key": "MQ==",
        "value": "MzA="
    },
    {
        "key": "Mg==",
        "value": "MTA="
    }
]
```

## Produce
```shell script
curl -X POST \
  http://localhost:8082/topics/topic-input \
  -H 'accept: application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json' \
  -H 'content-type: application/json' \
  -d '{
  "records": [
      {
          "key": "MQ==",
          "value": "MTA="
      },
      {
          "key": "MQ==",
          "value": "MjA="
      },
      {
          "key": "MQ==",
          "value": "MzA="
      },
      {
          "key": "Mg==",
          "value": "MTA="
      }
  ]
}'
```

## Consume

```shell script
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \        
    --topic aggregation-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer
```
