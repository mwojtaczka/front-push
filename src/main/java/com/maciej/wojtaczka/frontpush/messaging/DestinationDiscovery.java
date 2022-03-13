package com.maciej.wojtaczka.frontpush.messaging;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Component
class DestinationDiscovery {

    private final ReactiveStringRedisTemplate redisTemplate;

    public DestinationDiscovery(ReactiveStringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    Mono<String> discoverForUser(UUID userId) {
        return redisTemplate.opsForValue().get(userId.toString());
    }
}
