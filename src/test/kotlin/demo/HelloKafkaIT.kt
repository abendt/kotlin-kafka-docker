package demo

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.*
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import java.util.*

class HelloKafkaIT {

    @get:Rule
    private val kafkaBrokerRule = KafkaTopicRule()

    private val inputTopic = "inputTopic"

    private val outputTopic = "outputTopic"

    @Before
    fun setUp() {
        try {
            kafkaBrokerRule.createTopic(inputTopic)
            kafkaBrokerRule.createTopic(outputTopic)
        } catch (e: TopicExistsException) {
        }
    }

    @Test
    fun shouldUppercaseTheInput() {
        val inputValues = listOf("hello", "", "world")
        val expectedValues = listOf("HELLO", "WORLD")

        val builder = StreamsBuilder()

        val streamsConfiguration = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-lambda-integration-test")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerRule.bootstrapServers())
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().javaClass.name)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
        }
        val input: KStream<ByteArray, String> = builder.stream(inputTopic)
        val uppercased = input.mapValues { it.toUpperCase() }
        val filtered = uppercased.filter { _, value -> !value.isBlank() }
        filtered.to(outputTopic)

        val streams = KafkaStreams(builder.build(), streamsConfiguration)
        streams.start()

        //
        // Step 2: Produce some input data to the input topic.
        //
        val producerConfig = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerRule.bootstrapServers())
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.RETRIES_CONFIG, 0)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        }
        IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues, producerConfig)

        //
        // Step 3: Verify the application's output data.
        //
        val consumerConfig = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerRule.bootstrapServers())
            put(ConsumerConfig.GROUP_ID_CONFIG, "map-function-lambda-integration-test-standard-consumer")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
        }

        val actualValues: List<String> = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerConfig,
                outputTopic, expectedValues.size)
        streams.close()
        assert.that(actualValues, equalTo(expectedValues))
    }
}