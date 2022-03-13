package com.maciej.wojtaczka.frontpush.model.parcel;

import lombok.Value;

import java.util.Collection;
import java.util.UUID;

@Value
public class OutboundParcel<T> {

	Type type;
	Collection<UUID> receivers;
	T payload;

	public static <E> OutboundParcel<E> pack(E payload, Type type, Collection<UUID> receivers) {
		return new OutboundParcel<>(type, receivers, payload);
	}

	public enum Type {
		MESSAGE,
		MESSAGE_STATUS
	}
}
