package com.github.andrelugomes

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

object MapApp {
    const val INPUT_TOPIC = "map-input"
    const val OUTPUT_TOPIC = "mapped-output"

    @JvmStatic
    fun main(args: Array<String>) {
        val properties = Properties()
        val builder = StreamsBuilder()

        builder.stream<String, String>(INPUT_TOPIC)
            .map { key, value -> KeyValue(key, value.toInt().times(2).toString()) }
            .to(OUTPUT_TOPIC)

        val topology = builder.build()
        print(topology.describe())

        /**
         * Topologies:
            Sub-topology: 0
            Source: KSTREAM-SOURCE-0000000000 (topics: [map-input])
            --> KSTREAM-MAP-0000000001
            Processor: KSTREAM-MAP-0000000001 (stores: [])
            --> KSTREAM-SINK-0000000002
            <-- KSTREAM-SOURCE-0000000000
            Sink: KSTREAM-SINK-0000000002 (topic: mapped-output)
            <-- KSTREAM-MAP-0000000001
         */

        //Properties
        properties.putAll(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "map-app",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )

        val streams = KafkaStreams(topology, properties)
        val latch = CountDownLatch(1)

        // attach shutdown handler to catch control-c
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
