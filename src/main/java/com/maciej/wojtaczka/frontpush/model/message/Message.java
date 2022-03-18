package com.maciej.wojtaczka.frontpush.model.message;

import lombok.Builder;
import lombok.Value;
import lombok.With;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

@Value
@Builder
@With
//TODO move it to the test dir
public class Message {

    UUID authorId;
	Instant time;
	String content;
	UUID conversationId;
	Set<UUID> seenBy;
}
