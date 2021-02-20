package com.andrelugomes.dsl.operations.stateful

import io.confluent.kafka.serializers.KafkaJsonDeserializer
import io.confluent.kafka.serializers.KafkaJsonSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

object MerchantEventRouting {
    const val SOURCE_TOPIC = "events"

    /**
        ./kafka-topics.sh --create --topic events \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1
     */

    /**
        ./kafka-topics.sh --create --topic merchant-order-count-events \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1
     */

    /**
        ./kafka-topics.sh --create --topic merchant-menu-access-count-events \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1
     */

    /**
      ./kafka-topics.sh --create --topic merchant-menu-access-count \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1
     */

    /**
        ./kafka-topics.sh --create --topic merchant-order-count \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1
     */

    /**
        ./kafka-console-producer.sh --topic events --bootstrap-server localhost:9092 --property "parse.key=true" --property "key.separator=:"
     */

    /**
      ./kafka-console-consumer.sh --bootstrap-server localhost:9092 \
        --topic merchant-menu-access-count \
        --from-beginning \
        --property print.key=true \
        --property print.value=true \
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
     */

    /**
        ./kafka-console-consumer.sh --bootstrap-server localhost:9092 \
        --topic merchant-order-count \
        --from-beginning \
        --property print.key=true \
        --property print.value=true \
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
     */

    @JvmStatic
    fun main(args: Array<String>) {

        val properties = Properties()
        properties.putAll(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "merchant-event-routing",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG to StreamsConfig.EXACTLY_ONCE,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )

        val builder = StreamsBuilder()
        val forks = builder.stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), MerchantSerdes.Click()))
            .branch(
                { _ , value -> value.place.equals("ORDER")}, //0
                { _ , value -> value.place.equals("MENU")}, //1
                { _ , _ -> true }, //2
            )

        forks[0].to("merchant-order-count-events")
        forks[1].to("merchant-menu-access-count-events")
        forks[2].to("events-unknown")

        val topology = builder.build()
        val streams = KafkaStreams(topology, properties)
        val latch = CountDownLatch(1)

        Runtime.getRuntime().addShutdownHook(object : Thread("shutdown-hook") {
            override fun run() {
                streams.close()
                latch.countDown()
            }
        })

        try {
            streams.start()
            latch.await()
        } catch (e: Throwable) {
            exitProcess(1)
        }
        exitProcess(0)
    }
}

class MerchantSerdes{

    companion object {

        @JvmStatic
        fun Click(): Serde<Click>? {
            val serdeProps = mapOf(
                "json.value.type" to Click::class.java
            )

            val serializer = KafkaJsonSerializer<Click>()
            val deserializer = KafkaJsonDeserializer<Click>()

            serializer.configure(serdeProps, false)
            deserializer.configure(serdeProps, false)

            return Serdes.serdeFrom(serializer, deserializer)
        }
    }
}

data class Click(var place: String? = "")
