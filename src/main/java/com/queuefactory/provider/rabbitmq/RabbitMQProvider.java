package com.queuefactory.provider.rabbitmq;

import java.util.Map;

import com.queuefactory.util.Util;
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
	
	public RabbitMQProvider(String host) {
		try {
			factory = new ConnectionFactory();
			factory.setHost(host);
			connection = factory.newConnection();
			channel = connection.createChannel();
		} catch (Exception e) {
			log.error("RabbitMQ ERROR connection not established", e);
		}
	}
	
	public static RabbitMQProvider connect(String host) {
		return new RabbitMQProvider(host);
	}

	/**
	 * Envio de mensagem diretamente para uma Queue
	 * @param queueName - nome da fila
	 * @param durable - true se estivermos declarando uma fila durável (a fila sobreviverá à reinicialização do servidor)
	 * @param exclusive -  true se estamos declarando uma fila exclusiva (restrita a esta conexão)
	 * @param autoDelete - true se estivermos declarando uma fila de autodetecção (o servidor a excluirá quando não estiver mais em uso)
	 * @param arguments - outras propriedades (argumentos de construção) para a fila
	 * @param message - mensagem a ser enviada
	 */
	public void sendMessageQueue(String queueName, Boolean durable, Boolean exclusive, Boolean autoDelete, Map<String, Object> arguments, byte[] message) {
		try {
			channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments);
			channel.basicPublish("", queueName, null, message);
			log.info("RabbitMQ SUCESS send message to Queue: {}", queueName);
		} catch (Exception e) {
			log.error("RabbitMQ ERROR send message to Queue: {}", queueName , e);
		} finally {
			close();
		}
	}
	
	/**
	 * Envio de mensagem diretamente para uma Queue
	 * @param queueName - nome da fila
	 * @param durable - true se estivermos declarando uma fila durável (a fila sobreviverá à reinicialização do servidor)
	 * @param exclusive -  true se estamos declarando uma fila exclusiva (restrita a esta conexão)
	 * @param autoDelete - true se estivermos declarando uma fila de autodetecção (o servidor a excluirá quando não estiver mais em uso)
	 * @param arguments - outras propriedades (argumentos de construção) para a fila
	 * @param message - mensagem a ser enviada
	 */
	public void sendMessageQueue(String queueName, Boolean durable, Boolean exclusive, Boolean autoDelete, Map<String, Object> arguments, Object message) {
		try {
			channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments);
			channel.basicPublish("", queueName, null, Util.getByteArray(message));
			log.info("RabbitMQ SUCESS send message to Queue: {}", queueName);
		} catch (Exception e) {
			log.error("RabbitMQ ERROR send message to Queue: {}", queueName , e);
		} finally {
			close();
		}
	}
	
	/**
	 * Envio de mensagem para exchange
	 * @param exchangeName - nome da exchange
	 * @param exchangeType - type exchange
	 * @param exchangeDurable - true se estamos declarando uma exchange durável (a exchange sobreviverá à reinicialização do servidor)
	 * @param routingKey - routing key
	 * @param message - mensagem a ser enviada
	 */
	public void sendMessageExchange(String exchangeName, BuiltinExchangeType exchangeType, Boolean exchangeDurable, String routingKey, byte[] message) {
		try {
			routingKey = routingKey == null ? "" : routingKey;
			channel.exchangeDeclare(exchangeName, exchangeType, exchangeDurable);
			channel.basicPublish(exchangeName, routingKey, null, message);
			log.info("RabbitMQ SUCESS send message to Exchange: {}, Type: {}, Routing Key: {} ", exchangeName, exchangeType.name(), routingKey);
		} catch (Exception e) {
			log.error("RabbitMQ ERROR send message to Exchange: {}, Type: {}, Routing Key: {} ", exchangeName, exchangeType.name(), routingKey, e);
		} finally {
			close();
		}
	}
	
	/**
	 * Envio de mensagem para exchange
	 * @param exchangeName - nome da exchange
	 * @param exchangeType - type exchange
	 * @param exchangeDurable - true se estamos declarando uma exchange durável (a exchange sobreviverá à reinicialização do servidor)
	 * @param routingKey - routing key
	 * @param message - object mensagem a ser enviada
	 */
	public void sendMessageExchange(String exchangeName, BuiltinExchangeType exchangeType, Boolean exchangeDurable, String routingKey, Object message) {
		try {
			routingKey = routingKey == null ? "" : routingKey;
			channel.exchangeDeclare(exchangeName, exchangeType, exchangeDurable);
			channel.basicPublish(exchangeName, routingKey, null, Util.getByteArray(message));
			log.info("RabbitMQ SUCESS send message to Exchange: {}, Type: {}, Routing Key: {} ", exchangeName, exchangeType.name(), routingKey);
		} catch (Exception e) {
			log.error("RabbitMQ ERROR send message to Exchange: {}, Type: {}, Routing Key: {} ", exchangeName, exchangeType.name(), routingKey, e);
		} finally {
			close();
		}
	}

	/**
	 * Abre um canal para ouvir novas menssagens que chegam na fila
	 * @param queueName - queue name
	 * @param durable -  true se estivermos declarando uma fila durável (a fila sobreviverá à reinicialização do servidor)
	 * @param exclusive - true se estamos declarando uma fila exclusiva (restrita a esta conexão)
	 * @param autoDelete - true se estivermos declarando uma fila de autodetecção (o servidor a excluirá quando não estiver mais em uso)
	 * @param arguments -  outras propriedades (argumentos de construção) para a fila
	 * @return canal para ouvir novas menssagens
	 */
	public Channel listeningQueue(String queueName, Boolean durable, Boolean exclusive, Boolean autoDelete, Map<String, Object> arguments) {
		try {
			channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments);
			log.info("RabbitMQ SUCESS listening Queue: {}", queueName);
			return channel;
		} catch (Exception e) {
			log.error("RabbitMQ ERROR listening Queue: {}", queueName , e);
			return null;
		} 
    }
	
	void close() {
		try {
			channel.close();
			connection.close();
		} catch (Exception e) {
			log.error("RabbitMQ ERROR close connection", e);
		}
	}
}
