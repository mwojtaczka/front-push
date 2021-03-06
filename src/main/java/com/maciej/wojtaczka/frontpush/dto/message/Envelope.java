package com.maciej.wojtaczka.frontpush.dto.message;

import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

@AllArgsConstructor
@Value
public class Envelope<T> {
    T payload;
    List<UUID> recipients;

    public static <T> Envelope<T> wrap(T payload, Collection<UUID> interlocutors) {
        return new Envelope<>(payload, List.copyOf(interlocutors));
    }

    public List<UUID> getRecipients() {
        return List.copyOf(recipients);
    }
}
