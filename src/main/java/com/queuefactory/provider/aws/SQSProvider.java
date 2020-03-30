package com.queuefactory.provider.aws;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.queuefactory.util.Util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SQSProvider {
	
	private final AWSCredentials credentials;
	private final AmazonSQS sqsClient;
	
	public SQSProvider(String accessKey, String secretKey) {
		credentials = new BasicAWSCredentials(accessKey, secretKey);
		sqsClient = AmazonSQSClientBuilder
					.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials))
					.withRegion(Regions.SA_EAST_1).build();
	}
	
	public static SQSProvider connect(String accessKey, String secretKey) {
        return new SQSProvider(accessKey, secretKey);
    }
	
	public void createQueue(SQSTypeQueue type, String queueName) {
		try {
			CreateQueueResult queueResult = null;
			
			if(type.equals(SQSTypeQueue.AWS_SQS_STANDARD)) {
				queueResult = createStandardQueue(queueName);
				
			} else if(type.equals(SQSTypeQueue.AWS_SQS_FIFO)) {
				queueResult = createFifoQueue(queueName);
				
			} else if(type.equals(SQSTypeQueue.AWS_SQS_DEAD_LETTER)) {
				queueResult = createDeadLetterQueue(queueName);
			}
			
			if(queueResult.getSdkHttpMetadata().getHttpStatusCode() == 200) {
				log.info("AWS SQS SUCESS create queue Type: {} Name: {} - URL: {}", type.name(), queueName, queueResult.getQueueUrl());
				
			} else {
				log.error("AWS SQS ERROR create queue Type: {} Name: {}", type.name(), queueName);
			}
		} catch (Exception e) {
			log.error("AWS SQS ERROR create queue Type: {} Name: {}", type.name(), queueName, e);
		}
	}
	
	private CreateQueueResult createStandardQueue(String queueName) {
		return sqsClient.createQueue(new CreateQueueRequest(queueName));
	}
	
	private CreateQueueResult createFifoQueue(String queueName) {
		Map<String, String> queueAttributes = new HashMap<String, String>();
        queueAttributes.put("FifoQueue", "true");
        queueAttributes.put("ContentBasedDeduplication", "true");
        return sqsClient.createQueue(new CreateQueueRequest(queueName +".fifo").withAttributes(queueAttributes));
	}
	
	private CreateQueueResult createDeadLetterQueue(String queueName) {
		CreateQueueResult queueResult = sqsClient.createQueue(queueName);
		
		String queueUrlDeadLetter = sqsClient.createQueue(queueName+ "-dead-letter").getQueueUrl();
		
		GetQueueAttributesResult deadLetterQueueAttributes = sqsClient
				.getQueueAttributes(new GetQueueAttributesRequest(queueUrlDeadLetter)
				.withAttributeNames("QueueArn"));
		
		String deadLetterQueueARN = deadLetterQueueAttributes.getAttributes().get("QueueArn");

		SetQueueAttributesRequest queueAttributesRequest = new SetQueueAttributesRequest()
				.withQueueUrl(queueResult.getQueueUrl())
				.addAttributesEntry("RedrivePolicy", "{\"maxReceiveCount\":\"2\", " + "\"deadLetterTargetArn\":\"" + deadLetterQueueARN + "\"}");

		sqsClient.setQueueAttributes(queueAttributesRequest);
		return queueResult;
	}

	public void sendMessage(String queueName, String message) {
        sqsClient.sendMessage(new SendMessageRequest(queueName, message));
		log.info("SQS Sucess send message queue name: {} ", queueName);
	}
	
	public void sendMessageObject(String queueName, Object message) {
        try {
			sqsClient.sendMessage(new SendMessageRequest(queueName, Util.serializeToBase64(message)));
			log.info("SQS Sucess send message queue name: {} ", queueName);
		} catch (IOException e) {
			log.error("SQS ERROR send message queue name: {} ", queueName, e);
		}
	}

	/**
	 * Leitura de mensagem
	 * @param queueName - nome da fila
	 * @param waitTimeSeconds - duração (em segundos) pela qual a chamada aguarda a chegada de uma mensagem na fila antes de retornar. 
	 * Se uma mensagem estiver disponível, a chamada retornará mais cedo que WaitTimeSeconds. 
	 * Se nenhuma mensagem estiver disponível e o tempo de espera expirar, a chamada retornará com sucesso com uma lista vazia de mensagens.
	 * @return List<Message> list de mensagens
	 */
	public List<Message> readMessages(String queueName, Integer waitTimeSeconds) {
		String queueUrl = sqsClient.getQueueUrl(queueName).getQueueUrl();
		ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl)
				.withWaitTimeSeconds(waitTimeSeconds) 
				.withMaxNumberOfMessages(10);
		return sqsClient.receiveMessage(request).getMessages();
	}
	
	/**
	 * Leitura de mensagem
	 * @param <T> - Object a ser deserializado
	 * @param queueName - nome da fila
	 * @param waitTimeSeconds - duração (em segundos) pela qual a chamada aguarda a chegada de uma mensagem na fila antes de retornar. 
	 * Se uma mensagem estiver disponível, a chamada retornará mais cedo que WaitTimeSeconds. 
	 * Se nenhuma mensagem estiver disponível e o tempo de espera expirar, a chamada retornará com sucesso com uma lista vazia de mensagens.
	 * @return mensagem deserializada
	 */
	@SuppressWarnings("unchecked")
	public <T> List<T> readMessagesObject(String queueName, Integer waitTimeSeconds) {
		String queueUrl = sqsClient.getQueueUrl(queueName).getQueueUrl();
		ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl)
				.withWaitTimeSeconds(waitTimeSeconds) 
				.withMaxNumberOfMessages(10);
		
		List<Message> sqsMessages = sqsClient.receiveMessage(request).getMessages();
		
		List<T> list = new ArrayList<T>();
		sqsMessages.forEach(m -> {
			try {
				list.add((T) Util.deserializeFromBase64(m.getBody()));
				deleteMessageQueue(queueName, m.getReceiptHandle());
				log.info("SQS Sucess read message queue name: {}, msg: {} ", queueName, m.getMessageId());
			} catch (Exception e) {
				log.error("SQS ERROR read message queue name: {}, msg: {} ", queueName, m.getMessageId(), e);
			}
		});
		return list;
	}

	private void deleteMessageQueue(String queueName, String messageID) {
		String queueUrl = sqsClient.getQueueUrl(queueName).getQueueUrl();
		sqsClient.deleteMessage(new DeleteMessageRequest()
				.withQueueUrl(queueUrl)
				.withReceiptHandle(messageID));
	}

	public List<String> listQueues() {
		return sqsClient.listQueues().getQueueUrls();	
	}

	public String getUrlQueue(String queueName) {
		return sqsClient.getQueueUrl(queueName).getQueueUrl();
	}
}
