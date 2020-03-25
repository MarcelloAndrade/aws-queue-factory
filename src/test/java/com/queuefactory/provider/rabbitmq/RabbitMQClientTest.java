package com.queuefactory.provider.rabbitmq;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.queuefactory.examples.User;
import com.rabbitmq.client.BuiltinExchangeType;

@SpringBootTest()
public class RabbitMQClientTest {
	
	private static final String HOST = "localhost";
	private static final String QUEUE_NAME = "queue-test";
	private static final String DIRECT_EXCHANGE_NAME = "direct-exchange-test";
	private static final String FANOUT_EXCHANGE_NAME = "fanout-exchange-test";
	
	@Test
	public void test_send_message_queue_channel() {
		byte[] message = "message test".getBytes();
		RabbitMQProvider.builder(HOST).sendMessageQueue(QUEUE_NAME, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, null, message);
	}
	
	@Test
	public void test_send_message_direct_exchange() {
		byte[] message = "message test".getBytes();
		RabbitMQProvider.builder(HOST).sendMessageExchange(DIRECT_EXCHANGE_NAME, BuiltinExchangeType.DIRECT, Boolean.TRUE, "routingKey-1", message);
		RabbitMQProvider.builder(HOST).sendMessageExchange(DIRECT_EXCHANGE_NAME, BuiltinExchangeType.DIRECT, Boolean.TRUE, "routingKey-2", message);
		RabbitMQProvider.builder(HOST).sendMessageExchange(DIRECT_EXCHANGE_NAME, BuiltinExchangeType.DIRECT, Boolean.TRUE, "routingKey-2", message);
		RabbitMQProvider.builder(HOST).sendMessageExchange(DIRECT_EXCHANGE_NAME, BuiltinExchangeType.DIRECT, Boolean.TRUE, "routingKey-2", message);
	}
	
	@Test
	public void test_send_message_fanout_exchange() {
		byte[] message = "message test".getBytes();
		RabbitMQProvider.builder(HOST).sendMessageExchange(FANOUT_EXCHANGE_NAME, BuiltinExchangeType.FANOUT, Boolean.TRUE, null, message);
	}
	
	@Test
	public void test_send_message_fanout_exchange_object() {
		User user = new User();
		user.setName("Maria");
		user.setAge(31);
		RabbitMQProvider.builder(HOST).sendMessageExchange(FANOUT_EXCHANGE_NAME, BuiltinExchangeType.FANOUT, Boolean.TRUE, null, user);
	}
	
}
