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

    /**
      ./kafka-topics.sh --create --topic aggregation-grouped-table \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 1
     */

    /**
      ./kafka-console-consumer.sh --bootstrap-server localhost:9092 \
        --topic aggregation-grouped-table \
        --from-beginning \
        --property print.key=true \
        --property print.value=true \
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.DoubleDeserializer
     */
    const val SINK_TOPIC = "aggregation-grouped-table"

    @JvmStatic
    fun main(args: Array<String>) {

        //Configuration Properties
        val properties = Properties()
        properties.putAll(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "sales-aggregation-grouped-table",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG to StreamsConfig.EXACTLY_ONCE,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )

        val builder = StreamsBuilder()
        val table: KTable<String, Sales> = builder.table(SOURCE_TOPIC, Consumed.with(Serdes.String(), CustomSerdes.Sales()))

        table.toStream().peek { key, value -> println("key=${key}, value=${value}") }

        val groupedTable: KGroupedTable<String, Double> = table.groupBy(
            { _, sales -> KeyValue(sales.country, sales.amount)},
            Grouped.with(Serdes.String(), Serdes.Double())
        )

        /**
         * joe:{"country":"brazil", "amount":100.0} //brazil	100.0
         * mike:{"country":"mexico", "amount":10.0} //mexico	10.0
         *
         * joe:{"country":"argentina", "amount":1.0} //brazil	0.0  e argentina	1.0

         * mike:{"country":"brazil", "amount":11.0} // mexico	0.0  e brazil	11.0
         * anna:{"country":"brazil", "amount":1.0} //brazil	12.0
         *
         * joe:null //argentina	0.0
         * mike:null //brazil	1.0
         * anna:null //brazil	0.0
         */
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
            .toStream()
            .peek { key, value -> println("key=${key}, value=${value}") }// CDC
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
