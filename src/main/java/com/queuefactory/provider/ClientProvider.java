package com.queuefactory.provider;

import java.util.List;
import java.util.Map;

import com.amazonaws.services.sqs.model.Message;

public interface ClientProvider {
	
	public void createQueue(TypeQueue type, String queueName);

	public void sendMessage(TypeQueue type, String queueName, Map<String, String> message);
	
	public void sendMessages(TypeQueue type, String queueName,List<Map<String, String>> messages);
	
	public List<Message> readMessagesQueue(TypeQueue type, String queueName);

	public void deleteMessageQueue(TypeQueue type, String queueName, String messageID);
	
	public void monitoring(TypeQueue type, String queueName);
	
	public List<String> listQueues();

	public String getUrlQueue(String queueName);
}
