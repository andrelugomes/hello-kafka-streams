package com.andrelugomes.dsl.operations.stateful

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

object MerchantMenuAccessCount {
    const val SOURCE_TOPIC = "merchant-menu-access-count-events"
    const val SINK_TOPIC = "merchant-menu-access-count"

    @JvmStatic
    fun main(args: Array<String>) {

        val properties = Properties()
        properties.putAll(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "merchant-menu-access-counter",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG to StreamsConfig.EXACTLY_ONCE,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )

        val builder = StreamsBuilder()
        val stream = builder.stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), MerchantSerdes.Click()))

        stream.filter { _ , value -> value.place.equals("MENU") } //safe rule
            .groupByKey()
            .count()
            .toStream()
            .to(SINK_TOPIC, Produced.with(Serdes.String(), Serdes.Long()))

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
