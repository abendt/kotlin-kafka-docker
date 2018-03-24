package demo.eventtracking

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

data class TrackingEvent(val name: String)

object JACKSON {
    val mapper = ObjectMapper()
            .registerModule(KotlinModule())
            .registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
}

class TrackingEventSerializer : Serializer<TrackingEvent> {

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    }

    override fun serialize(topic: String, data: TrackingEvent): ByteArray {
        return JACKSON.mapper.writeValueAsBytes(data)
    }

    override fun close() {
    }
}

class TrackingEventDeserializer : Deserializer<TrackingEvent> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    }

    override fun deserialize(topic: String, data: ByteArray): TrackingEvent {
        return JACKSON.mapper.readValue(data, TrackingEvent::class.java)
    }

    override fun close() {
    }
}

class TrackingEventSerde : Serde<TrackingEvent> {
    val ser: TrackingEventSerializer  by lazy { TrackingEventSerializer() }
    val deser: TrackingEventDeserializer by lazy { TrackingEventDeserializer() }

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    }

    override fun deserializer(): Deserializer<TrackingEvent> {
        return deser
    }

    override fun close() {
    }

    override fun serializer(): Serializer<TrackingEvent> {
        return ser
    }
}