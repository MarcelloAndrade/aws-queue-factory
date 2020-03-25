package com.queuefactory.provider.rabbitmq;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

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
	
	public static RabbitMQProvider builder(String host) {
		return new RabbitMQProvider(host);
	}

	/**
	 * Envio de menssagem diretamente para uma Queue
	 * @param queueName - nome da fila
	 * @param durable - true se estivermos declarando uma fila durável (a fila sobreviverá à reinicialização do servidor)
	 * @param exclusive -  true se estamos declarando uma fila exclusiva (restrita a esta conexão)
	 * @param autoDelete - true se estivermos declarando uma fila de autodetecção (o servidor a excluirá quando não estiver mais em uso)
	 * @param arguments - outras propriedades (argumentos de construção) para a fila
	 * @param message - menssagem a ser enviada
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
	 * Envio de menssagem para exchange
	 * @param exchangeName - nome da exchange
	 * @param exchangeType - type exchange
	 * @param exchangeDurable - true se estamos declarando uma troca durável (a troca sobreviverá à reinicialização do servidor)
	 * @param routingKey - routing key
	 * @param message - menssagem a ser enviada
	 */
	public void sendMessageExchange(String exchangeName, BuiltinExchangeType exchangeType, Boolean exchangeDurable, String routingKey, byte[] message) {
		try {
			routingKey = routingKey == null ? "" : routingKey;
			channel.exchangeDeclare(exchangeName, exchangeType, exchangeDurable);
			channel.basicPublish(exchangeName, routingKey, null, message);
			log.info("RabbitMQ SUCESS send message to Exchange: {}, Type: {}, Routing Key: {} ", exchangeName, exchangeType.name(), routingKey);
		} catch (Exception e) {
			log.error("RabbitMQ SUCESS send message to Exchange: {}, Type: {}, Routing Key: {} ", exchangeName, exchangeType.name(), routingKey, e);
		} finally {
			close();
		}
	}
	
	/**
	 * Envio de menssagem para exchange
	 * @param exchangeName - nome da exchange
	 * @param exchangeType - type exchange
	 * @param exchangeDurable - true se estamos declarando uma troca durável (a troca sobreviverá à reinicialização do servidor)
	 * @param routingKey - routing key
	 * @param message - object menssagem a ser enviada
	 */
	public void sendMessageExchange(String exchangeName, BuiltinExchangeType exchangeType, Boolean exchangeDurable, String routingKey, Object message) {
		try {
			routingKey = routingKey == null ? "" : routingKey;
			channel.exchangeDeclare(exchangeName, exchangeType, exchangeDurable);
			channel.basicPublish(exchangeName, routingKey, null, getByteArray(message));
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
	
	private byte[] getByteArray(Object obj) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(out);
		os.writeObject(obj);
		return out.toByteArray();
	}
	
	public void close() {
		try {
			channel.close();
			connection.close();
		} catch (Exception e) {
			log.error("RabbitMQ ERROR close connection", e);
		}
	}
}
