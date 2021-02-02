package com.andrelugomes.concepts

import com.andrelugomes.dsl.operations.stateful.CustomSerdes
import com.andrelugomes.dsl.operations.stateful.Sales
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

object Table {
    const val SOURCE_TOPIC = "input-sales"
    const val SINK_TOPIC = "table-compact"

    @JvmStatic
    fun main(args: Array<String>) {

        //Configuration Properties
        val properties = Properties()
        properties.putAll(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "table-compact",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG to StreamsConfig.EXACTLY_ONCE,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )

        /**
         * brazil:{"country":"brazil", "amount":100.0}
         * mexico:{"country":"mexico", "amount":10.0}
         *
         * argentina:{"country":"argentina", "amount":1.0}
         * brazil:{"country":"brazil", "amount":2.0}
         *
         * brazil:null
         * null:{"country":"argentina", "amount":2.0}
         * :{"country":"argentina", "amount":2.0}
         *
         */

        val builder = StreamsBuilder()

        //Read from kafka as a Table
        val table: KTable<String, Sales> = builder.table(
            SOURCE_TOPIC, Consumed.with(Serdes.String(),
                CustomSerdes.Sales()
            ))
        table.toStream().to(SINK_TOPIC, Produced.with(Serdes.String(), CustomSerdes.Sales()))


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
