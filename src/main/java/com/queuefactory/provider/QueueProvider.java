package com.queuefactory.provider;

import java.util.List;
import java.util.Map;

import com.amazonaws.services.sqs.model.Message;

import lombok.NonNull;

public class QueueProvider {
	
	private final ClientProvider clientProvider;
	
	public QueueProvider(@NonNull ClientProvider clientProvider) {
		this.clientProvider = clientProvider;
	}
	
	public static QueueProvider connect(@NonNull ClientProvider clientProvider) {
        return new QueueProvider(clientProvider);
    }
	
	public void createQueue(TypeQueue type, String queueName) {
		clientProvider.createQueue(type, queueName);
	}

	public void sendMessage(TypeQueue type, String queueName, Map<String, String> message) {
		clientProvider.sendMessage(type, queueName, message);
	}

	public void sendMessages(TypeQueue type, String queueName,List<Map<String, String>> messages) {
		clientProvider.sendMessages(type, queueName, messages);
	}

	public List<Message> readMessagesQueue(TypeQueue type, String queueName) {
		return clientProvider.readMessagesQueue(type, queueName);
	}

	public void deleteMessageQueue(TypeQueue type, String queueName, String messageID) {
		clientProvider.deleteMessageQueue(type, queueName, messageID);
	}

	public void monitoring(TypeQueue type, String queueName) {
		clientProvider.monitoring(type, queueName);
	}

	public List<String> listQueues() {
		return clientProvider.listQueues();
	}
	
	public String getUrlQueue(String queueName) {
		return clientProvider.getUrlQueue(queueName);
	}

}
