package com.queuefactory.provider.rabbitmq;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest()
public class RabbitMQClientTest {
	
	private static final String QUEUE_NAME = "queue-test1";
	private static final String TOPIC_EXCHANGE_NAME = "topic-exchange-test";
	private static final String FANOUT_EXCHANGE_NAME = "fanout-exchange-test";
	
	@Test
	public void testSendMessageQueueChannel() {
		byte[] message = "message test".getBytes();
		RabbitMQProvider.builder().sendMessageQueueChannel(QUEUE_NAME, message);
	}
	
	@Test
	public void testSendMessageTopicExchange() {
		byte[] message = "message test".getBytes();
		RabbitMQProvider.builder().sendMessageTopicExchange(TOPIC_EXCHANGE_NAME, "routingKey-1", message);
		RabbitMQProvider.builder().sendMessageTopicExchange(TOPIC_EXCHANGE_NAME, "routingKey-2", message);
		RabbitMQProvider.builder().sendMessageTopicExchange(TOPIC_EXCHANGE_NAME, "routingKey-2", message);
	}
	
	@Test
	public void testSendMessageFanoutExchange() {
		byte[] message = "message test".getBytes();
		RabbitMQProvider.builder().sendMessageFanoutExchange(FANOUT_EXCHANGE_NAME, "routingKey-1", message);
	}
	
}
