package com.queuefactory.examples;

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
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

public class AwsSqs {

	private static final String STANDARD_QUEUE_NAME = "queue-factory";
	private static final String FIFO_QUEUE_NAME = "queue-factory.fifo";
	private static final String DEAD_LETTER_QUEUE_NAME = "queue-factory-dead-letter";
	
	private static final AWSCredentials credentials;

	static {
		credentials = new BasicAWSCredentials("accessKey", "secretKey");
	}

	public static void main(String[] args) {
		AmazonSQS sqsClient = AmazonSQSClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withRegion(Regions.SA_EAST_1).build();
		
		// CREATE
		createStandardQueue(sqsClient, STANDARD_QUEUE_NAME);
		createFifoQueue(sqsClient, FIFO_QUEUE_NAME);
		createDeadLetterQueue(sqsClient, DEAD_LETTER_QUEUE_NAME);
		
		// SEND MESSAGE
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put("AttributeOne", new MessageAttributeValue().withStringValue("This is an attribute").withDataType("String"));
        
		sendMessageStandardQueue(sqsClient, STANDARD_QUEUE_NAME, messageAttributes);
		sendMessageFifoQueue(sqsClient, FIFO_QUEUE_NAME, messageAttributes);
		
		List<SendMessageBatchRequestEntry> messageEntries = new ArrayList<>();
        messageEntries.add(new SendMessageBatchRequestEntry().withId("id-5").withMessageBody("batch-1").withMessageGroupId("group-1"));
        messageEntries.add(new SendMessageBatchRequestEntry().withId("id-6").withMessageBody("batch-2").withMessageGroupId("group-1"));
        
        sendMultipleMessages(sqsClient, FIFO_QUEUE_NAME, messageEntries);
        
        readMessageQueue(sqsClient, STANDARD_QUEUE_NAME);
		readMessageQueue(sqsClient, FIFO_QUEUE_NAME);
		
//		deleteMessageQueue(sqsClient, STANDARD_QUEUE_NAME, "");
		
		monitoring(sqsClient, STANDARD_QUEUE_NAME);
		monitoring(sqsClient, FIFO_QUEUE_NAME);
		
		listQueues(sqsClient);
	}
	
	public static void createStandardQueue(AmazonSQS sqsClient, String queueName) {
		CreateQueueRequest queueRequest = new CreateQueueRequest(queueName);
		CreateQueueResult queueResult = sqsClient.createQueue(queueRequest);
		String queueUrl = queueResult.getQueueUrl();
		System.out.println(queueUrl);
	}
	
	public static void createFifoQueue(AmazonSQS sqsClient, String queueName) {
		Map<String, String> queueAttributes = new HashMap<String, String>();
        queueAttributes.put("FifoQueue", "true");
        queueAttributes.put("ContentBasedDeduplication", "true");

        CreateQueueRequest queueRequest = new CreateQueueRequest(queueName).withAttributes(queueAttributes);;
		CreateQueueResult queueResult = sqsClient.createQueue(queueRequest);
		String queueUrl = queueResult.getQueueUrl();
		System.out.println(queueUrl);
	}
	
	public static void createDeadLetterQueue(AmazonSQS sqsClient, String queueName) {
		String queueUrl = sqsClient.createQueue(queueName).getQueueUrl();
		
		String queueUrlDeadLetter = sqsClient.createQueue(queueName+ "-redrect").getQueueUrl();
		
		GetQueueAttributesResult deadLetterQueueAttributes = sqsClient
				.getQueueAttributes(new GetQueueAttributesRequest(queueUrlDeadLetter)
				.withAttributeNames("QueueArn"));
		
		String deadLetterQueueARN = deadLetterQueueAttributes.getAttributes().get("QueueArn");

		SetQueueAttributesRequest queueAttributesRequest = new SetQueueAttributesRequest()
				.withQueueUrl(queueUrl)
				.addAttributesEntry("RedrivePolicy", "{\"maxReceiveCount\":\"2\", " + "\"deadLetterTargetArn\":\"" + deadLetterQueueARN + "\"}");

		sqsClient.setQueueAttributes(queueAttributesRequest);
	}
	
	public static void sendMessageStandardQueue(AmazonSQS sqsClient, String queueName, Map<String, MessageAttributeValue> messageAttributes) {
		String queueUrl = sqsClient.getQueueUrl(queueName).getQueueUrl();
        SendMessageRequest sendMessageStandardQueue = new SendMessageRequest().withQueueUrl(queueUrl)
	            .withMessageBody("A simple message.")
	            //.withDelaySeconds(30) 
	            .withMessageAttributes(messageAttributes);
        sqsClient.sendMessage(sendMessageStandardQueue);
	}
	
	public static void sendMessageFifoQueue(AmazonSQS sqsClient, String queueName, Map<String, MessageAttributeValue> messageAttributes) {
		String queueUrl = sqsClient.getQueueUrl(queueName).getQueueUrl();
		SendMessageRequest sendMessageFifoQueue = new SendMessageRequest().withQueueUrl(queueUrl)
				.withMessageBody("FIFO Queue")
				.withMessageGroupId("pilot-group-1")
				.withMessageAttributes(messageAttributes);
		sqsClient.sendMessage(sendMessageFifoQueue);
	}
	
	public static void sendMultipleMessages(AmazonSQS sqsClient, String queueName, List<SendMessageBatchRequestEntry> messages) {
		String queueUrl = sqsClient.getQueueUrl(queueName).getQueueUrl();
        SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(queueUrl, messages);
        sqsClient.sendMessageBatch(sendMessageBatchRequest);
	}
	
	public static List<Message> readMessageQueue(AmazonSQS sqsClient, String queueName) {
		String queueUrl = sqsClient.getQueueUrl(queueName).getQueueUrl();
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
				.withWaitTimeSeconds(10) 
				.withMaxNumberOfMessages(5); // Max is 10

		List<Message> sqsMessages = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
		sqsMessages.forEach(m -> System.out.println(" Message ID: " +m.getMessageId()+ 
													" Attribute: " +m.getAttributes()+
													" Body: "+m.getBody()+
													" Receipt Handle: " +m.getReceiptHandle()
													));
		return sqsMessages;
	}
	
	public static void deleteMessageQueue(AmazonSQS sqsClient, String queueName, String messageReceiptHandle) {
		String queueUrl = sqsClient.getQueueUrl(queueName).getQueueUrl();
		sqsClient.deleteMessage(new DeleteMessageRequest()
				.withQueueUrl(queueUrl)
				.withReceiptHandle(messageReceiptHandle));
	}
	
	public static void monitoring(AmazonSQS sqsClient, String queueName) {
		String queueUrl = sqsClient.getQueueUrl(queueName).getQueueUrl();
		GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest(queueUrl).withAttributeNames("All");
		GetQueueAttributesResult getQueueAttributesResult = sqsClient.getQueueAttributes(getQueueAttributesRequest);
		System.out.println(String.format("The number of messages on the queue: %s", getQueueAttributesResult.getAttributes().get("ApproximateNumberOfMessages")));
		System.out.println(String.format("The number of messages in flight: %s", getQueueAttributesResult.getAttributes().get("ApproximateNumberOfMessagesNotVisible")));
	}
	
	public static void listQueues(AmazonSQS sqsClient) {
		ListQueuesResult result = sqsClient.listQueues();
		System.out.println("Your SQS Queue URLs:");
		result.getQueueUrls().forEach(r -> System.out.println(r));
	}
}
