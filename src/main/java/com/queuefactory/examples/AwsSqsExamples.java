package com.queuefactory.examples;

import java.util.List;

import com.amazonaws.services.sqs.model.Message;
import com.queuefactory.provider.aws.SQSProvider;
import com.queuefactory.provider.aws.SQSTypeQueue;

class AwsSqsExamples {
	
	private static final String ACCESS_KEY = "access_key";
	private static final String SECRET_KEY = "secret_key";	
	
	private static final String STANDARD_QUEUE_NAME = "queue-test1";
	private static final String FIFO_QUEUE_NAME = "queue-test2";
	private static final String DEAD_LETTER_QUEUE_NAME = "queue-test3";
	
	public static void main(String[] args) {
		exampleCreateStandardQueue();
		exampleCreateFifoQueue();
		exampleCreateDeadLetterQueue();
		
		exampleSendMessage();
		exampleReadMessages();
		
		exampleSendMessageObject();
		exampleReadMessagesObject();
	}
	
	public static void exampleCreateStandardQueue() {
		SQSProvider.connect(ACCESS_KEY, SECRET_KEY).createQueue(SQSTypeQueue.AWS_SQS_STANDARD, STANDARD_QUEUE_NAME);
	}
	
	public static void exampleCreateFifoQueue() {
		SQSProvider.connect(ACCESS_KEY, SECRET_KEY).createQueue(SQSTypeQueue.AWS_SQS_FIFO, FIFO_QUEUE_NAME);
	}
	
	public static void exampleCreateDeadLetterQueue() {
		SQSProvider.connect(ACCESS_KEY, SECRET_KEY).createQueue(SQSTypeQueue.AWS_SQS_DEAD_LETTER, DEAD_LETTER_QUEUE_NAME);
	}
	
	public static void exampleSendMessage() {
		SQSProvider.connect(ACCESS_KEY, SECRET_KEY).sendMessage(STANDARD_QUEUE_NAME, "Test message");
	}
	
	public static void exampleReadMessages() {
		List<Message> sqsMessages = SQSProvider.connect(ACCESS_KEY, SECRET_KEY).readMessages(STANDARD_QUEUE_NAME, 10);
		sqsMessages.forEach(m -> System.out.println(" Message ID: " +m.getMessageId()+ 
													" Attribute: " +m.getAttributes()+
													" Body: "+m.getBody()+
													" Receipt Handle: " +m.getReceiptHandle()
													));
	}
	
	public static void exampleSendMessageObject() {
		User user = new User();
		user.setName("Maria");
		user.setAge(31);
		SQSProvider.connect(ACCESS_KEY, SECRET_KEY).sendMessageObject(DEAD_LETTER_QUEUE_NAME, user);
	}
	
	public static void exampleReadMessagesObject() {
		List<User> sqsMessages = SQSProvider.connect(ACCESS_KEY, SECRET_KEY).readMessagesObject(DEAD_LETTER_QUEUE_NAME, 10);
		sqsMessages.forEach(m -> System.out.println(" Name: " +m.getName()+ 
													" Age: " +m.getAge()
													));
	}
}
