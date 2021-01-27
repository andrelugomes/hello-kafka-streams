package com.andrelugomes.dsl.operations.stateful

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Produced
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

object GroupBy {
    const val SOURCE_TOPIC = "input"
    const val SINK_TOPIC = "output"

    @JvmStatic
    fun main(args: Array<String>) {

        //Configuration Properties
        val properties = Properties()
        properties.putAll(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "filter",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )

        //KGroupedStream
        val builder = StreamsBuilder()
        builder.stream<String, String>(SOURCE_TOPIC)
            //.groupBy { _, value -> value}
            //.groupBy {key, _ -> key}
            .groupByKey()
            .count()
            //.aggregate {} //****stateful****
            //.windowedBy {} //****stateful****
            //.reduce { _, value -> value } //****stateful****
            .toStream() // Convert a KTable to KStreams
            .to(SINK_TOPIC, Produced.valueSerde(Serdes.Long()))

        //Build Topology of stream
        val topology = builder.build()
        print(topology.describe())

        val streams = KafkaStreams(topology, properties)
        val latch = CountDownLatch(1)

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(object : Thread("shutdown-hook") {
            override fun run() {
                streams.close()
                latch.countDown()
            }
        })

        //Start...
        try {
            streams.start()
            latch.await()
        } catch (e: Throwable) {
            exitProcess(1)
        }
        exitProcess(0)
    }
}
