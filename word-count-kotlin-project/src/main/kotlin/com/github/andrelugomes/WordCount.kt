package com.github.andrelugomes

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Produced
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

object WordCount {
    const val INPUT_TOPIC = "streams-plaintext-input"
    const val OUTPUT_TOPIC = "streams-wordcount-output"

    @JvmStatic
    fun main(args: Array<String>) {
        val properties = Properties()
        val builder = StreamsBuilder()

        builder.stream<String, String>(INPUT_TOPIC)
            .flatMapValues { value -> listOf(*value.toLowerCase().split(" ".toRegex()).toTypedArray()) }
            .groupBy { _ , value -> value }
            .count()
            .toStream()
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()))

        //Properties
        properties.putAll(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "streams-wordcount-kotlin",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )

        val topology = builder.build()
        val streams = KafkaStreams(topology, properties)
        val latch = CountDownLatch(1)

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(object : Thread("streams-wordcount-shutdown-hook") {
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
