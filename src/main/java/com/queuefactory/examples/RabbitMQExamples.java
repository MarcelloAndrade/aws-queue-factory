package com.queuefactory.examples;

import java.io.IOException;

import com.queuefactory.provider.rabbitmq.RabbitMQProvider;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class RabbitMQExamples {
	
	private static final String HOST = "localhost";
	private static final String QUEUE_NAME = "queue-test";
	private static final String QUEUE_NAME_OBJECT = "queue-test-object";
	
	private static final String FANOUT_QUEUE_NAME_1 = "fanout-queue-1";
	
	private static final String DIRECT_EXCHANGE_NAME = "direct-exchange-test";
	private static final String FANOUT_EXCHANGE_NAME = "fanout-exchange-test";
	
	public static void main(String[] args) {
		exampleSendMessageQueue();
		exampleReadMessageQueue();
		
		exampleSendMessageQueueObject();
		exampleReadMessageQueueObject();
		
		exampleSendMessageDirectExchange();
		exampleSendMessageFanoutExchange();
	}
	
	public static void exampleSendMessageQueue() {
		byte[] message = "message test".getBytes();
		RabbitMQProvider.connect(HOST).sendMessageQueue(QUEUE_NAME, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, null, message);
	}
	
	public static void exampleReadMessageQueue() {
		try {
			Channel channel = RabbitMQProvider.connect(HOST).listeningQueue(QUEUE_NAME, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, null);
			channel.basicConsume(QUEUE_NAME, Boolean.TRUE, "consumer-tag-1", new DefaultConsumer(channel) {
			    @Override
			    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
			    	String message = new String(body, "UTF-8");
			    	log.info("RabbitMQ SUCESS read message Queue: {}, Message: {} ", QUEUE_NAME, message);
			    }
			 });
		} catch (IOException e) {
			log.error("RabbitMQ ERROR read message Queue: {}", QUEUE_NAME);
		} 
	}
	
	public static void exampleSendMessageQueueObject() {
		User user = new User();
		user.setName("Maria");
		user.setAge(31);
		RabbitMQProvider.connect(HOST).sendMessageQueue(QUEUE_NAME_OBJECT, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, null, user);
	}
	
	public static void exampleReadMessageQueueObject() {
		try {
			Channel channel = RabbitMQProvider.connect(HOST).listeningQueue(QUEUE_NAME_OBJECT, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, null);
			channel.basicConsume(QUEUE_NAME_OBJECT, Boolean.TRUE, "consumer-tag-2", new DefaultConsumer(channel) {
			    @Override
			    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
					try {
						User message = (User) RabbitMQProvider.deserialize(body); 
						log.info("RabbitMQ SUCESS read message Queue: {}, Message: {} ", QUEUE_NAME_OBJECT, message);
					} catch (ClassNotFoundException | IOException e) {
						log.error("RabbitMQ ERROR read message Queue: {}", QUEUE_NAME_OBJECT, e);
					}
			    }
			 });
		} catch (IOException e) {
			log.error("RabbitMQ ERROR read message Queue: {}", FANOUT_QUEUE_NAME_1, e);
		} 
	}
	
	public static void exampleSendMessageDirectExchange() {
		byte[] message = "message test".getBytes();
		RabbitMQProvider.connect(HOST).sendMessageExchange(DIRECT_EXCHANGE_NAME, BuiltinExchangeType.DIRECT, Boolean.TRUE, "routingKey-1", message);
		RabbitMQProvider.connect(HOST).sendMessageExchange(DIRECT_EXCHANGE_NAME, BuiltinExchangeType.DIRECT, Boolean.TRUE, "routingKey-2", message);
		RabbitMQProvider.connect(HOST).sendMessageExchange(DIRECT_EXCHANGE_NAME, BuiltinExchangeType.DIRECT, Boolean.TRUE, "routingKey-2", message);
		RabbitMQProvider.connect(HOST).sendMessageExchange(DIRECT_EXCHANGE_NAME, BuiltinExchangeType.DIRECT, Boolean.TRUE, "routingKey-2", message);
	}
	
	public static void exampleSendMessageFanoutExchange() {
		byte[] message = "message test".getBytes();
		RabbitMQProvider.connect(HOST).sendMessageExchange(FANOUT_EXCHANGE_NAME, BuiltinExchangeType.FANOUT, Boolean.TRUE, null, message);
	}
	
}
