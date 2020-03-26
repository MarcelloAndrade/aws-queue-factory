package com.queuefactory.examples;

import com.queuefactory.provider.aws.SQSProvider;
import com.queuefactory.provider.aws.SQSTypeQueue;

class AwsSqsExamples {
	
	private static final String ACCESS_KEY = "xxxxxxx";
	private static final String SECRET_KEY = "yyyyyyy";	
	
	private static final String STANDARD_QUEUE_NAME = "queue-test1";
	private static final String FIFO_QUEUE_NAME = "queue-test2";
	private static final String DEAD_LETTER_QUEUE_NAME = "queue-test3";
	
	public static void main(String[] args) {
		exampleCreateStandardQueue();
		exampleCreateFifoQueue();
		exampleCreateDeadLetterQueue();
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
}
