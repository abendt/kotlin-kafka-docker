package demo

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.hasSize
import demo.eventtracking.TrackingEvent
import demo.eventtracking.TrackingEventDeserializer
import demo.eventtracking.TrackingEventSerde
import demo.eventtracking.TrackingEventSerializer
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
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import java.util.*

class UniqueVisitorIT {

    @get:Rule
    private val kafkaBrokerRule = KafkaTopicRule()

    private val inputTopic = UUID.randomUUID().toString()

    private val outputTopic = UUID.randomUUID().toString()

    @Before
    fun setUp() {
        try {
            kafkaBrokerRule.createTopic(inputTopic)
            kafkaBrokerRule.createTopic(outputTopic)
        } catch (e: TopicExistsException) {
        }
    }

    @After
    fun tearDown() {
        kafkaBrokerRule.deleteTopic(inputTopic)
        kafkaBrokerRule.deleteTopic(outputTopic)
    }

    @Test
    fun shouldUppercaseTheInput() {
        val inputValues = listOf(TrackingEvent("user2"), TrackingEvent("user1"), TrackingEvent("user1"), TrackingEvent("user1"))

        val builder = StreamsBuilder()

        val streamsConfiguration = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "unique-visitors")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerRule.bootstrapServers())
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TrackingEventSerde::class.java)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
        }

        val input: KStream<String, TrackingEvent> = builder.stream(inputTopic)

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
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TrackingEventSerializer::class.java)
        }
        IntegrationTestUtils.produceKeyValuesSynchronously(inputTopic, inputValues.map { KeyValue(it.name, it) }, producerConfig)

        //
        // Step 3: Verify the application's output data.
        //
        val consumerConfig = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerRule.bootstrapServers())
            put(ConsumerConfig.GROUP_ID_CONFIG, "unique-visitors-consumer")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TrackingEventDeserializer::class.java)
        }

        val actual = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived<String, TrackingEvent>(consumerConfig, outputTopic, 2)
        streams.close()

        println(actual)

        assert.that(actual, hasSize(equalTo(2)))
    }

}