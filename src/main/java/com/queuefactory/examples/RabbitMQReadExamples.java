package com.queuefactory.examples;

import java.io.IOException;

import com.queuefactory.provider.rabbitmq.RabbitMQProvider;
import com.queuefactory.provider.rabbitmq.RabbitMQUtil;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RabbitMQReadExamples {
	
	private static final String HOST = "localhost";
	private static final String QUEUE_NAME = "queue-test";
	private static final String FANOUT_QUEUE_NAME_1 = "fanout-queue-1";
	private static final String FANOUT_QUEUE_NAME_2 = "fanout-queue-2";
	
	public static void main(String[] args) {
//		exampleReadMessageQueue();
//		exampleReadMessageQueue1();
//		exampleReadMessageQueue2();
		exampleReadMessageQueueObject();
	}
	
	public static void exampleReadMessageQueue() {
		try {
			Channel channel = RabbitMQProvider.builder(HOST).listeningQueue(QUEUE_NAME, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, null);
			channel.basicConsume(QUEUE_NAME, Boolean.TRUE, "ConsumerTag", new DefaultConsumer(channel) {
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
	
	public static void exampleReadMessageQueue1() {
		try {
			Channel channel = RabbitMQProvider.builder(HOST).listeningQueue(FANOUT_QUEUE_NAME_1, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, null);
			channel.basicConsume(FANOUT_QUEUE_NAME_1, Boolean.TRUE, "ConsumerTagFanout", new DefaultConsumer(channel) {
			    @Override
			    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
			    	String message = new String(body, "UTF-8");
			    	log.info("RabbitMQ SUCESS read message Queue: {}, Message: {} ", FANOUT_QUEUE_NAME_1, message);
			    }
			 });
		} catch (IOException e) {
			log.error("RabbitMQ ERROR read message Queue: {}", FANOUT_QUEUE_NAME_1);
		} 
	}
	
	public static void exampleReadMessageQueue2() {
		try {
			Channel channel = RabbitMQProvider.builder(HOST).listeningQueue(FANOUT_QUEUE_NAME_2, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, null);
			channel.basicConsume(FANOUT_QUEUE_NAME_2, Boolean.TRUE, "ConsumerTagFanout", new DefaultConsumer(channel) {
			    @Override
			    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
			    	String message = new String(body, "UTF-8");
			    	log.info("RabbitMQ SUCESS read message Queue: {}, Message: {} ", FANOUT_QUEUE_NAME_2, message);
			    }
			 });
		} catch (IOException e) {
			log.error("RabbitMQ ERROR read message Queue: {}", FANOUT_QUEUE_NAME_2);
		} 
	}
	
	public static void exampleReadMessageQueueObject() {
		try {
			Channel channel = RabbitMQProvider.builder(HOST).listeningQueue(FANOUT_QUEUE_NAME_1, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, null);
			channel.basicConsume(FANOUT_QUEUE_NAME_1, Boolean.TRUE, "ConsumerTagFanout", new DefaultConsumer(channel) {
			    @Override
			    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
					try {
						User message = (User) RabbitMQUtil.deserialize(body);
						log.info("RabbitMQ SUCESS read message Queue: {}, Message: {} ", FANOUT_QUEUE_NAME_1, message);
					} catch (ClassNotFoundException | IOException e) {
						log.error("RabbitMQ ERROR read message Queue: {}", FANOUT_QUEUE_NAME_1, e);
					}
			    }
			 });
		} catch (IOException e) {
			log.error("RabbitMQ ERROR read message Queue: {}", FANOUT_QUEUE_NAME_1, e);
		} 
	}
	
}
