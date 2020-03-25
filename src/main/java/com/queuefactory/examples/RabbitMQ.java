package com.queuefactory.examples;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RabbitMQ {

	private static final String QUEUE_NAME = "queue-test";
	private static final String FANOUT_EXCHANGE_NAME = "fanout-exchange-test";
	private static final String FANOUT_QUEUE_NAME_1 = "fanout-queue-1";
	private static final String FANOUT_QUEUE_NAME_2 = "fanout-queue-2";

	public static void main(String[] args) throws IOException, TimeoutException {
//		sendMessageQueue();
//		readMessageQueue();
//		sendMessageFanoutExchange();
		createExchangeAndBindingQueue();
	}

	public static void sendMessageQueue() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		
		Channel channel = connection.createChannel();
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);

		String message = "product details";
		channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
		System.out.println(" [x] Sent '" + message + "'");

		channel.close();
		connection.close();
	}
	
	public static void readMessageQueue() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
            }
        };
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }
	
	public static void sendMessageFanoutExchange() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		
		Channel channel = connection.createChannel();
		channel.exchangeDeclare(FANOUT_EXCHANGE_NAME, BuiltinExchangeType.FANOUT, Boolean.TRUE);
		
		String message = "product details";
		channel.basicPublish(FANOUT_EXCHANGE_NAME, "", null, message.getBytes(StandardCharsets.UTF_8));
		System.out.println(" [x] Sent '" + message + "'");

		channel.close();
		connection.close();
	}
	
	public static void createExchangeAndBindingQueue() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		// CRIA EXCHANTE
		channel.exchangeDeclare(FANOUT_EXCHANGE_NAME, BuiltinExchangeType.FANOUT, Boolean.TRUE);
		// CRIA QUEUE
		channel.queueDeclare(FANOUT_QUEUE_NAME_1, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, null);
		channel.queueDeclare(FANOUT_QUEUE_NAME_2, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE, null);
		// BIND QUEUE
		channel.queueBind(FANOUT_QUEUE_NAME_1, FANOUT_EXCHANGE_NAME, "");
		channel.queueBind(FANOUT_QUEUE_NAME_2, FANOUT_EXCHANGE_NAME, "");
		
		String message = "product details";
		channel.basicPublish(FANOUT_EXCHANGE_NAME, "", null, message.getBytes(StandardCharsets.UTF_8));
		System.out.println(" [x] Sent '" + message + "'");

		channel.close();
		connection.close();
	}

}
