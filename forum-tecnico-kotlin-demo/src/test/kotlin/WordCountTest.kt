
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.*
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.Produced
import org.hamcrest.CoreMatchers.equalTo

import java.util.*
import org.junit.*
import org.junit.Assert.assertThat

class WordCountTest {

    /*@ClassRule
    val CLUSTER = EmbeddedKafkaCluster(1)*/

    lateinit var inputTopic: TestInputTopic<String, String>
    lateinit var outputTopic: TestOutputTopic<String, Long>
    lateinit var testDriver: TopologyTestDriver

    /*@BeforeClass
    fun startKafkaCluster() {
        CLUSTER.createTopic(inputTopic);
        CLUSTER.createTopic(outputTopic);
    }*/

    @Before
    fun setup() {
        val properties = Properties()
        properties.putAll(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to "test",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "dummy:9092",
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG to 0,
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
            )
        )

        val builder = StreamsBuilder()
        builder.stream<String, String>("input-topic")
            .flatMapValues { value -> listOf(*value.toLowerCase().split(" ".toRegex()).toTypedArray()) }
            .groupBy { _ , value -> value }
            .count()
            .toStream()
            .to("result-topic", Produced.with(Serdes.String(), Serdes.Long()))


        testDriver = TopologyTestDriver(builder.build(), properties)

        inputTopic = testDriver.createInputTopic("input-topic", Serdes.String().serializer(), Serdes.String().serializer())
        outputTopic = testDriver.createOutputTopic("result-topic", Serdes.String().deserializer(), Serdes.Long().deserializer())
    }

    @Test
    fun test(){
        inputTopic.pipeInput("1", "Super Kafka Testing")
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue("super", 1L)))
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue("kafka", 1L)))
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue("testing", 1L)))

        inputTopic.pipeInput("2", "Kafka Streams Testing")
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue("kafka", 2L)))
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue("streams", 1L)))
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue("testing", 2L)))
    }


    @After
    fun tearDown() {
        testDriver.close()
    }
}