package com.andrelugomes.dsl.operations.stateless

import com.andrelugomes.dsl.operations.stateful.CustomSerdes
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

object Branch {
    private const val SOURCE_TOPIC = "input-sales"
    private const val SINK_TOPIC_0 = "output-brazil"
    private const val SINK_TOPIC_1 = "output-argentina"
    private const val SINK_TOPIC_2 = "output-mexico"
    private const val SINK_TOPIC_UNKNOWN = "output-unknown"

    @JvmStatic
    fun main(args: Array<String>) {

        //Configuration Properties
        val properties = Properties()
        properties.putAll(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "branch",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )

        //Stream processing builder
        val builder = StreamsBuilder()

        /**
         * Split the Stream by Predicates one by one, If no predicates match, the record will be drop
         *
         *
         * brazil:{"country":"brazil", "amount":100.0}
         * mexico:{"country":"mexico", "amount":10.0}
         * argentina:{"country":"argentina", "amount":1.0}
         * peru:{"country":"peru", "amount":1.0}
         */
        var forks = builder.stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), CustomSerdes.Sales()))
            .branch(
                { key, _ -> key.equals("brazil")}, //first predicate
                { key, _ -> key.equals("argentina")}, //second
                { key, _ -> key.equals("mexico")}, //third
                { _, _ -> true }, //fourth - everything else
            )

        forks[0].to(SINK_TOPIC_0)
        forks[1].to(SINK_TOPIC_1)
        forks[2].to(SINK_TOPIC_2)
        forks[3].to(SINK_TOPIC_UNKNOWN)

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
