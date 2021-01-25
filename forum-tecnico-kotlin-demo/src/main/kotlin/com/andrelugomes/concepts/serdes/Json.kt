package com.andrelugomes.concepts.serdes

import io.confluent.kafka.serializers.KafkaJsonDeserializer
import io.confluent.kafka.serializers.KafkaJsonSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess


object Json {
    const val SOURCE_TOPIC = "temperature-input"
    const val SINK_TOPIC = "high-temperature-output"

    @JvmStatic
    fun main(args: Array<String>) {

        //Configuration Properties
        val properties = Properties()
        properties.putAll(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "temperature-alarm",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )

        //Json Serde
        val serdeProps = mapOf(
            "json.value.type" to Temperature::class.java
        )

        val temperatureSerializer = KafkaJsonSerializer<Temperature>()
        val temperatureDeserializer = KafkaJsonDeserializer<Temperature>()

        temperatureSerializer.configure(serdeProps, false)
        temperatureDeserializer.configure(serdeProps, false)

        val tempSerde = Serdes.serdeFrom(temperatureSerializer, temperatureDeserializer)
        val stringSerde = Serdes.String()

        //Stream processing builder
        val builder = StreamsBuilder()
        builder.stream(SOURCE_TOPIC, Consumed.with(stringSerde, tempSerde))
            .filter { _, measure -> measure.temperature > 25 }
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

class Temperature{
    lateinit var station: String
    var temperature: Double = 0.0
    var timestamp: Long = 0
}
