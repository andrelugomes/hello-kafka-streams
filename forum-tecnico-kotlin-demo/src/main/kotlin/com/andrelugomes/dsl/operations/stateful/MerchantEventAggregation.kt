package com.andrelugomes.dsl.operations.stateful

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

object MerchantEventAggregation {
    const val SOURCE_TOPIC = "events"

    const val SINK_TOPIC = "events"

    @JvmStatic
    fun main(args: Array<String>) {

        val properties = Properties()
        properties.putAll(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "merchant-event-aggregation",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG to StreamsConfig.EXACTLY_ONCE,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )

        val builder = StreamsBuilder()
        val forks = builder
            .stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), MerchantSerdes.Click()))
            .branch(
                { _ , value -> value.place.equals("ORDER")},
                { _ , value -> value.place.equals("MENU")},
                { _ , _ -> true },
            )

        val orders: KTable<String, Click> = forks[0].toTable(Named.`as`("orders-table"))
        val menus: KTable<String, Click> = forks[1].toTable(Named.`as`("menus-table"))
        forks[2].to("events-unknown")

        /*orders.toStream().groupByKey().count().toStream().to("merchant-order-count")
        menus.toStream().groupByKey().count().toStream().to("merchant-menu-access-count")*/

        val ordersGrouped: KGroupedStream<String, Click> = orders.toStream().groupByKey()
        val menusGrouped: KGroupedStream<String, Click> = menus.toStream().groupByKey()

        //ordersGrouped.cogroup().co


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
