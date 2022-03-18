package com.maciej.wojtaczka.frontpush.messaging;

import com.maciej.wojtaczka.frontpush.model.message.Envelope;
import com.maciej.wojtaczka.frontpush.model.parcel.OutboundParcel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;

import java.util.Map;

@Slf4j
class EnvelopeListener {

	private final ReactiveKafkaConsumerTemplate<String, Envelope<?>> kafkaEnvelopeListener;
	private final Forwarder forwarder;
	private final Map<String, OutboundParcel.Type> topicToTypeMap;

	public EnvelopeListener(ReactiveKafkaConsumerTemplate<String, Envelope<?>> kafkaMessagesListener,
							Forwarder forwarder,
							Map<String, OutboundParcel.Type> topicToTypeMap) {
		this.kafkaEnvelopeListener = kafkaMessagesListener;
		this.forwarder = forwarder;
		this.topicToTypeMap = topicToTypeMap;
	}

	void listen() {
		kafkaEnvelopeListener.receive()
							 .flatMap(consumerRecord -> forwarder.forwardOutboundParcel(consumerRecord.value(), resolveType(consumerRecord.topic())))
							 .onErrorContinue((throwable, o) -> log.error(throwable.getMessage()))
							 .subscribe();
	}

	private OutboundParcel.Type resolveType(String topicName) {
		OutboundParcel.Type type = topicToTypeMap.get(topicName);
		if (type == null) {
			throw new IllegalStateException("Topic name cannot be resolved");
		}
		return type;
	}
}
