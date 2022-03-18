package com.maciej.wojtaczka.frontpush.messaging;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maciej.wojtaczka.frontpush.dto.message.Envelope;
import com.maciej.wojtaczka.frontpush.dto.parcel.OutboundParcel;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

@Configuration
public class MessagingConfiguration {

    public static final String MESSAGE_ACCEPTED_TOPIC = "message-accepted";
    public static final String MESSAGE_STATUS_UPDATED_TOPIC = "message-status-updated";

    @Value("${spring.application.name}")
    private String applicationName;

    @Bean
    ReactiveKafkaConsumerTemplate<String, Envelope<?>> reactiveMessageConsumerTemplate(KafkaProperties kafkaProperties,
																							 ObjectMapper objectMapper) {
        ReceiverOptions<String, Envelope<?>> basicReceiverOptions = ReceiverOptions.create(kafkaProperties.buildConsumerProperties());
        ReceiverOptions<String, Envelope<?>> messageReceiverOptions =
                basicReceiverOptions.subscription(Set.of(MESSAGE_ACCEPTED_TOPIC, MESSAGE_STATUS_UPDATED_TOPIC))
                        .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, applicationName)
                        .consumerProperty(JsonDeserializer.USE_TYPE_INFO_HEADERS, false)
						.withValueDeserializer(new KafkaGenericDeserializer<>(objectMapper, new TypeReference<Envelope<?>>() {
						}));

        return new ReactiveKafkaConsumerTemplate<>(messageReceiverOptions);
    }

    @Bean
    ReactiveKafkaProducerTemplate<String, OutboundParcel<?>> reactiveKafkaProducerTemplate(
            KafkaProperties properties) {

        Map<String, Object> props = properties
                .buildProducerProperties();

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new ReactiveKafkaProducerTemplate<>(SenderOptions.create(props));
    }

	@Bean
	EnvelopeListener acceptedMessagesListener(ReactiveKafkaConsumerTemplate<String, Envelope<?>> reactiveMessageConsumerTemplate,
											  Forwarder forwarder) {
		Map<String, OutboundParcel.Type> topicToTypeMap = Map.of(
				MESSAGE_ACCEPTED_TOPIC, OutboundParcel.Type.MESSAGE,
				MESSAGE_STATUS_UPDATED_TOPIC, OutboundParcel.Type.MESSAGE_STATUS
		);
		var acceptedMessagesListener = new EnvelopeListener(reactiveMessageConsumerTemplate, forwarder, topicToTypeMap);
		acceptedMessagesListener.listen();
		return acceptedMessagesListener;
	}

	static class KafkaGenericDeserializer<T> implements Deserializer<T> {

		private final ObjectMapper mapper;
		private final TypeReference<T> typeReference;

		public KafkaGenericDeserializer(ObjectMapper mapper, TypeReference<T> typeReference) {
			this.mapper = mapper;
			this.typeReference = typeReference;
		}

		@Override
		public T deserialize(final String topic, final byte[] data) {
			if (data == null) {
				return null;
			}

			try {
				return mapper.readValue(data, typeReference);
			} catch (final IOException ex) {
				throw new SerializationException("Can't deserialize data [" + Arrays.toString(data) + "] from topic [" + topic + "]", ex);
			}
		}

		@Override
		public void close() {}

		@Override
		public void configure(final Map<String, ?> settings, final boolean isKey) {}
	}
}
