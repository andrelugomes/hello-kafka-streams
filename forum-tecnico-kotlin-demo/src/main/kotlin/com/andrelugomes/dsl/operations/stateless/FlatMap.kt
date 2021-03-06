package com.andrelugomes.dsl.operations.stateless

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

object FlatMap {
    private const val SOURCE_TOPIC = "input"
    private const val SINK_TOPIC = "output"

    @JvmStatic
    fun main(args: Array<String>) {

        //Configuration Properties
        val properties = Properties()
        properties.putAll(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "flat-map",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )

        //Stream processing builder
        val builder = StreamsBuilder()
        builder.stream<String, String>(SOURCE_TOPIC)
            .peek { key, value -> println("key=${key}, value=${value}") }
            //.flatMap { key, value -> value.split(",").map { KeyValue(key, it) } }
            .flatMapValues { value -> value.split(",") }
            .peek { key, value -> println("key=${key}, value=${value}") }
            .to(SINK_TOPIC)

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
