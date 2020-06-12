package com.github.andrelugomes

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Grouped
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

object AggregationApp {
    const val INPUT_TOPIC = "topic-input"
    const val OUTPUT_TOPIC = "aggregation-output"

    @JvmStatic
    fun main(args: Array<String>) {
        val properties = Properties()
        val builder = StreamsBuilder()

        builder.stream<String, String>(INPUT_TOPIC)
        .map { key, value -> KeyValue(key, value.toInt()) }
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
        .reduce { value1, value2 -> value1 + value2 }
        .toStream()
        .to(OUTPUT_TOPIC)

        val topology = builder.build()
        print(topology.describe())

        properties.putAll(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "aggregation-app",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )

        val streams = KafkaStreams(topology, properties)
        val latch = CountDownLatch(1)

        Runtime.getRuntime().addShutdownHook(object : Thread("aggregation-app-shutdown-hook") {
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
