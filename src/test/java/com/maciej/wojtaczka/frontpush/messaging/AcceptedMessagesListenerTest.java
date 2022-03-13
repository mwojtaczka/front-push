package com.maciej.wojtaczka.frontpush.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maciej.wojtaczka.frontpush.model.message.Envelope;
import com.maciej.wojtaczka.frontpush.model.message.Message;
import com.maciej.wojtaczka.frontpush.utils.KafkaTestListener;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static com.maciej.wojtaczka.frontpush.messaging.MessagingConfiguration.MESSAGE_ACCEPTED_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
@DirtiesContext
class AcceptedMessagesListenerTest {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private KafkaTestListener kafkaTestListener;

	@Autowired
	private RedisTemplate<String, String> redisTemplate;

	@Autowired
	private ObjectMapper objectMapper;

	@Test
	void shouldFanoutMessageToTopicsAssignedToReceivers() throws JsonProcessingException, ExecutionException, InterruptedException {
		//given
		kafkaTestListener.listenToTopic("topic1", 1);
		kafkaTestListener.listenToTopic("topic2", 1);

		Message message = Message.builder()
								 .authorId(UUID.randomUUID())
								 .content("Hello world")
								 .conversationId(UUID.randomUUID())
								 .seenBy(Set.of(UUID.randomUUID()))
								 .time(Instant.parse("2007-12-03T10:15:30.00Z"))
								 .build();
		UUID recipient1 = UUID.randomUUID();
		UUID recipient2 = UUID.randomUUID();
		UUID recipient3 = UUID.randomUUID();

		redisTemplate.opsForValue().set(recipient1.toString(), "topic1");
		redisTemplate.opsForValue().set(recipient2.toString(), "topic2");
		redisTemplate.opsForValue().set(recipient3.toString(), "topic2");

		Envelope<Message> toBeFanout = Envelope.wrap(message, Set.of(recipient1, recipient2, recipient3));
		String envelopeJson = objectMapper.writeValueAsString(toBeFanout);

		//when
		kafkaTemplate.send(MESSAGE_ACCEPTED_TOPIC, envelopeJson).get();

		//then
		Thread.sleep(100);
		String jsonFromTopic1 = kafkaTestListener.receiveContentFromTopic("topic1").orElseThrow();
		Envelope<Message> fromTopic1 = objectMapper.readValue(jsonFromTopic1, new TypeReference<>() {
		});
		assertThat(fromTopic1.getReceivers()).containsExactly(recipient1);
		assertThat(fromTopic1.getPayload().getContent()).isEqualTo("Hello world");

		String jsonFromTopic2 = kafkaTestListener.receiveContentFromTopic("topic2").orElseThrow();
		Envelope<Message> fromTopic2 = objectMapper.readValue(jsonFromTopic2, new TypeReference<>() {
		});
		assertThat(fromTopic2.getReceivers()).containsExactlyInAnyOrder(recipient2, recipient3);
		assertThat(fromTopic2.getPayload().getContent()).isEqualTo("Hello world");
	}

}
