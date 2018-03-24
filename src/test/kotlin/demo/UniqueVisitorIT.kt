package demo

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.natpryce.hamkrest.hasSize
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.TimeWindows
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import java.util.*
import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo


class UniqueVisitorIT {

    @get:Rule
    private val kafkaBrokerRule = KafkaTopicRule()

    private val inputTopic = "applicationEvents"

    private val outputTopic = "uniqueVisitors"

    companion object {
        val mapper = ObjectMapper()
                .registerModule(KotlinModule())
                .registerModule(JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }


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
        val inputValues = listOf(WebVisit("user2"), WebVisit("user1"), WebVisit("user1"), WebVisit("user1"))

        val builder = StreamsBuilder()

        val streamsConfiguration = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "unique-visitors")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerRule.bootstrapServers())
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
        }
        val input: KStream<String, String> = builder.stream(inputTopic)

        val oneMinuteWindowed = input.groupByKey()
                .windowedBy(TimeWindows.of(60 * 1000))
                .reduce({ value1, _ -> value1 })

        oneMinuteWindowed.toStream().map { key, value -> KeyValue(key.key(), value) }.to(outputTopic)

        val streams = KafkaStreams(builder.build(), streamsConfiguration)
        streams.start()

        //
        // Step 2: Produce some input data to the input topic.
        //
        val producerConfig = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerRule.bootstrapServers())
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.RETRIES_CONFIG, 0)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        }
        IntegrationTestUtils.produceKeyValuesSynchronously(inputTopic, inputValues.map { KeyValue(it.name, it.toString()) }, producerConfig)

        //
        // Step 3: Verify the application's output data.
        //
        val consumerConfig = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerRule.bootstrapServers())
            put(ConsumerConfig.GROUP_ID_CONFIG, "unique-visitors-consumer")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
        }

        val actualValues: List<String> = IntegrationTestUtils.readValues<String, String>(outputTopic, consumerConfig, 1)
        streams.close()

        assert.that(actualValues, hasSize(equalTo(2)))
    }

    data class WebVisit(val name: String)

}