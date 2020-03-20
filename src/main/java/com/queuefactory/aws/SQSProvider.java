package com.queuefactory.aws;

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
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.queuefactory.provider.ClientProvider;
import com.queuefactory.provider.TypeQueue;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SQSProvider implements ClientProvider {
	
//	@Value("${aws.sqs.accessKey}")
//	private String accessKey;
//	
//	@Value("${aws.sqs.secretKey}")
//	private String secretKey;
	
	private final AWSCredentials credentials;
	private final AmazonSQS sqsClient;
	
	public SQSProvider() {
		credentials = new BasicAWSCredentials("xxxxxx", "yyyyyy");
		sqsClient = AmazonSQSClientBuilder
					.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials))
					.withRegion(Regions.SA_EAST_1).build();
	}
	
	public static SQSProvider builder() {
        return new SQSProvider();
    }
	
	@Override
	public void createQueue(TypeQueue type, String queueName) {
		try {
			CreateQueueResult queueResult = null;
			
			if(type.equals(TypeQueue.AWS_SQS_STANDARD)) {
				queueResult = createStandardQueue(type, queueName);
				
			} else if(type.equals(TypeQueue.AWS_SQS_FIFO)) {
				queueResult = createFifoQueue(type, queueName);
				
			} else if(type.equals(TypeQueue.AWS_SQS_DEAD_LETTER)) {
				queueResult = createDeadLetterQueue(type, queueName);
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
	
	private CreateQueueResult createStandardQueue(TypeQueue type, String queueName) {
		return sqsClient.createQueue(new CreateQueueRequest(queueName));
	}
	
	private CreateQueueResult createFifoQueue(TypeQueue type, String queueName) {
		Map<String, String> queueAttributes = new HashMap<String, String>();
        queueAttributes.put("FifoQueue", "true");
        queueAttributes.put("ContentBasedDeduplication", "true");
        return sqsClient.createQueue(new CreateQueueRequest(queueName +".fifo").withAttributes(queueAttributes));
	}
	
	private CreateQueueResult createDeadLetterQueue(TypeQueue type, String queueName) {
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

	@Override
	public void sendMessage(TypeQueue type, String queueName, Map<String, String> message) {
		// TODO Auto-generated method stub
		log.info("Sucess send message queue name: {} ", queueName);
	}

	@Override
	public void sendMessages(TypeQueue type, String queueName, List<Map<String, String>> messages) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Message> readMessagesQueue(TypeQueue type, String queueName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteMessageQueue(TypeQueue type, String queueName, String messageID) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void monitoring(TypeQueue type, String queueName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<String> listQueues() {
		return sqsClient.listQueues().getQueueUrls();	
	}

	@Override
	public String getUrlQueue(String queueName) {
		return sqsClient.getQueueUrl(queueName).getQueueUrl();
	}
}
