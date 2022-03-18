package com.maciej.wojtaczka.frontpush.messaging;

import com.maciej.wojtaczka.frontpush.dto.message.Envelope;
import com.maciej.wojtaczka.frontpush.dto.parcel.OutboundParcel;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

@Component
class Forwarder {

	private final ReactiveKafkaProducerTemplate<String, OutboundParcel<?>> reactiveKafkaProducerTemplate;
	private final DestinationDiscovery destinationDiscovery;

	public Forwarder(ReactiveKafkaProducerTemplate<String, OutboundParcel<?>> reactiveKafkaProducerTemplate,
					 DestinationDiscovery destinationDiscovery) {
		this.reactiveKafkaProducerTemplate = reactiveKafkaProducerTemplate;
		this.destinationDiscovery = destinationDiscovery;
	}

	<T> Mono<Void> forwardOutboundParcel(Envelope<T> envelope, OutboundParcel.Type type) {
		return Flux.fromIterable(envelope.getRecipients())
				   .flatMap(userId -> destinationDiscovery.discoverForUser(userId)
														  .map(destination -> Map.entry(destination, userId)))
				   .collectMultimap(Map.Entry::getKey, Map.Entry::getValue)
				   .flatMap(destinationToUsers -> fanout(destinationToUsers, envelope.getPayload(), type));
	}

	private <T> Mono<Void> fanout(Map<String, Collection<UUID>> destinationToUsersMap, T payload, OutboundParcel.Type type) {
		return Flux.fromIterable(destinationToUsersMap.entrySet())
				   .map(entry -> Map.entry(entry.getKey(), OutboundParcel.pack(payload, type, entry.getValue())))
				   .flatMap(destinationToParcelEntry ->
									reactiveKafkaProducerTemplate.send(destinationToParcelEntry.getKey(), destinationToParcelEntry.getValue()))
				   .then();
	}
}
