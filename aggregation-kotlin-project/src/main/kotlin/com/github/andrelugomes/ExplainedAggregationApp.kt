package com.github.andrelugomes

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

object ExplainedAggregationApp {
    const val INPUT_TOPIC = "topic-input"
    const val OUTPUT_TOPIC = "aggregation-output"

    @JvmStatic
    fun main(args: Array<String>) {
        val properties = Properties()
        val builder = StreamsBuilder()

        //GRAPH

        //Source: TOPIC-INPUT (topics: [topic-input])
        //KStream
        val stream = builder.stream<String, String>(INPUT_TOPIC, Consumed.`as`("TOPIC-INPUT"))

        //Processor: MAPPING-STAGE (stores: [])
        //KStream
        val map = stream.map({ key, value -> KeyValue(key, value.toInt()) }, Named.`as`("MAPPING-STAGE"))

        //Processor: KSTREAM-FILTER-0000000005
        //https://kafka.apache.org/25/documentation/streams/developer-guide/dsl-api.html#aggregating
        //KGroupedStream - Group Processor
        // Default value serialization is String, change to Integer
        val groupBy = map.groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))

        //Processor: REDUCING-STAGE (stores: [KSTREAM-REDUCE-STATE-STORE-0000000002])
        //KTable - CDC
        val reduce = groupBy.reduce({ value1, value2 -> value1 + value2 },
            Named.`as`("REDUCING-STAGE"),
            Materialized.with(Serdes.String(), Serdes.Integer()))
        //val reduce = groupBy.reduce(Integer::sum)

        //Sink
        reduce.toStream().to(OUTPUT_TOPIC)

        val topology = builder.build()
        print(topology.describe())

        //Properties
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
