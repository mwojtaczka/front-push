package com.maciej.wojtaczka.frontpush.config;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class TestConfig {

	@Bean
	KafkaTemplate<String, String> kafkaTemplate(
			KafkaProperties properties) {
		DefaultKafkaProducerFactory<String, String> producerFactory =
				new DefaultKafkaProducerFactory<>(properties.buildProducerProperties(), new StringSerializer(), new StringSerializer());

		return new KafkaTemplate<>(producerFactory);
	}
}
