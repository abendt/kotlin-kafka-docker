package demo

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.hasSize
import demo.eventtracking.TrackingEvent
import demo.eventtracking.TrackingEventSerde
import demo.eventtracking.TrackingEventSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer
import org.apache.kafka.streams.kstream.internals.WindowedSerializer
import org.apache.kafka.streams.state.WindowStore
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

        val builder = StreamsBuilder()

        val streamsConfiguration = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "unique-visitors")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerRule.bootstrapServers())
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
        }

        val input: KStream<String, TrackingEvent> =
                builder.stream(inputTopic,
                        Consumed.with(Serdes.String(),
                                TrackingEventSerde()))

        val oneMinutePerUser: KTable<Windowed<String>, Long> = input
                .peek({ key, value -> println("-> $key = $value") }) // Debug Output
                .groupByKey()                                        // group events by user
                .windowedBy(TimeWindows.of(60 * 1000))       // within timewindow
                .aggregate({ 0L }, { _, _, _ -> 1L },                // collapse to value 1
                        Materialized.`as`<String, Long, WindowStore<Bytes, ByteArray>>("one-minute-per-user")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long()))


        val oneMinuteActiveUsers: KTable<Windowed<String>, Long> =
                oneMinutePerUser.groupBy({ windowedKey, value ->
                    KeyValue(
                            Windowed(
                                    windowedKey.window().toString(),
                                    windowedKey.window()
                            ),
                            value
                    )
                }, Serialized.with(windowedString,
                        Serdes.Long()))
                        .reduce({ l, r -> l + r },
                                { l, r -> l - r })

        oneMinuteActiveUsers
                .toStream()
                .peek({ key, value -> println("State: $key = $value") })

                .map({ key, value -> KeyValue(key.toString(), value.toString()) })

                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()))

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

        repeat(2) {
            val data = listOf(TrackingEvent("user"), TrackingEvent("user2"))
            IntegrationTestUtils.produceKeyValuesSynchronously(inputTopic, data.map { KeyValue(it.name, it) }, producerConfig)

            Thread.sleep(60)
        }

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

        val actual = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived<String, String>(consumerConfig, outputTopic, 1)
        streams.close()

        println(actual)

        assert.that(actual, hasSize(equalTo(1)))
    }

    companion object {
        val windowedString = Serdes.serdeFrom(WindowedSerializer(StringSerializer()),
                WindowedDeserializer(StringDeserializer()))
    }

}