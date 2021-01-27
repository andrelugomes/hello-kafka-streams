package com.andrelugomes.dsl.operations.stateful

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.KeyValueStore
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

object AggregateKGroupedTable {
    const val SOURCE_TOPIC = "input-sales"
    const val SINK_TOPIC = "aggregation"

    @JvmStatic
    fun main(args: Array<String>) {

        //Configuration Properties
        val properties = Properties()
        properties.putAll(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "sales-aggregation",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG to StreamsConfig.EXACTLY_ONCE,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )

        val builder = StreamsBuilder()
        val grouped: KGroupedStream<String, Sales> = builder
            .stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), CustomSerdes.Sales()))
            .groupByKey()

        /**
         * brazil:{"country":"brazil", "amount":100.0}
         * mexico:{"country":"mexico", "amount":10.0}
         *
         * argentina:{"country":"argentina", "amount":1.0}
         * brazil:{"country":"brazil", "amount":1.0}
         *
         * brazil:null
         * null:{"country":"argentina", "amount":2.0}
         * :{"country":"argentina", "amount":2.0}
         *
         */
        val table: KTable<String, Sales> = grouped.aggregate(
            { Sales() }, //initalizer
            { key, sale, total -> Sales(sale.country, total.amount?.plus(sale.amount!!)) }, //aggregator
            Materialized.with<String, Sales, KeyValueStore<Bytes, ByteArray>>(
                Serdes.String(),
                CustomSerdes.Sales()
            )  //State Store Materialized
        )

        val groupedTable: KGroupedTable<String?, Double?> = table.groupBy { key, value -> KeyValue(key, value.amount) }

        groupedTable
            .aggregate(
                { 0.0 }, //initalizer
                { key, amount, total -> total?.plus(amount!!) }, //adder
                { key, amount, total -> total?.minus(amount!!) }, //substractor
                Materialized.with<String, Double, KeyValueStore<Bytes, ByteArray>>(
                    Serdes.String(),
                    Serdes.Double()
                )  //State Store
            )
            .toStream() // CDC
            .to(SINK_TOPIC, Produced.with(Serdes.String(), Serdes.Double()))

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
