package com.queuefactory.provider.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RabbitMQProvider {
	
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	
	public RabbitMQProvider() {
		try {
			factory = new ConnectionFactory();
			factory.setHost("localhost");
			connection = factory.newConnection();
			channel = connection.createChannel();
		} catch (Exception e) {
			log.error("RabbitMQ ERROR connection not established", e);
		}
	}
	
	public static RabbitMQProvider builder() {
		return new RabbitMQProvider();
	}

	public void sendMessageQueueChannel(String queueName, byte[] message) {
		try {
			channel.queueDeclare(queueName, false, false, false, null);
			channel.basicPublish("", queueName, null, message);
			log.info("RabbitMQ SUCESS send message to Queue: {}", queueName);
		} catch (Exception e) {
			log.error("RabbitMQ ERROR send message to Queue: {}", queueName , e);
		} finally {
			close();
		}
	}
	
	public void sendMessageTopicExchange(String exchangeName, String routingKey, byte[] message) {
		try {
			channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC);
			channel.basicPublish(exchangeName, routingKey, null, message);
			log.info("RabbitMQ SUCESS send message to Exchange: {} and Routing Key: {} ", exchangeName, routingKey);
		} catch (Exception e) {
			log.error("RabbitMQ ERROR send message to Exchange: {} and Routing Key: {} ", exchangeName, routingKey, e);
		} finally {
			close();
		}
	}
	
	public void sendMessageFanoutExchange(String exchangeName, String routingKey, byte[] message) {
		try {
			channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT);
			channel.basicPublish(exchangeName, routingKey, null, message);
			log.info("RabbitMQ SUCESS send message to Exchange: {} and Routing Key: {} ", exchangeName, routingKey);
		} catch (Exception e) {
			log.error("RabbitMQ ERROR send message to Exchange: {} and Routing Key: {} ", exchangeName, routingKey, e);
		} finally {
			close();
		}
	}
	
	public void sendMessageDirectExchange(String exchangeName, String routingKey, byte[] message) {
		try {
			channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT);
			channel.basicPublish(exchangeName, routingKey, null, message);
			log.info("RabbitMQ SUCESS send message to Exchange: {} and Routing Key: {} ", exchangeName, routingKey);
		} catch (Exception e) {
			log.error("RabbitMQ ERROR send message to Exchange: {} and Routing Key: {} ", exchangeName, routingKey, e);
		} finally {
			close();
		}
	}
	
	private void close() {
		try {
			channel.close();
			connection.close();
		} catch (Exception e) {
			log.error("RabbitMQ ERROR close connection", e);
		}
	}
}
