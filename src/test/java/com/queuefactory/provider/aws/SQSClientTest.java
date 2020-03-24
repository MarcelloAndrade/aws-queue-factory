package com.queuefactory.provider.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest()
public class SQSClientTest {
	
	private static final String STANDARD_QUEUE_NAME = "queue-test1";
	private static final String FIFO_QUEUE_NAME = "queue-test2";
	private static final String DEAD_LETTER_QUEUE_NAME = "queue-test3";
	
	@Test
	public void testCreateStandardQueue() {
		SQSProvider.builder().createQueue(SQSTypeQueue.AWS_SQS_STANDARD, STANDARD_QUEUE_NAME);
		String urlQueue = SQSProvider.builder().getUrlQueue(STANDARD_QUEUE_NAME);
		String[] urlSplit = urlQueue.split("/");
		assertEquals(urlSplit[urlSplit.length -1], STANDARD_QUEUE_NAME);
	}
	
	@Test
	public void testCreateFifoQueue() {
		SQSProvider.builder().createQueue(SQSTypeQueue.AWS_SQS_FIFO, FIFO_QUEUE_NAME);
		String urlQueue = SQSProvider.builder().getUrlQueue(FIFO_QUEUE_NAME +".fifo");
		String[] urlSplit = urlQueue.split("/");
		assertEquals(urlSplit[urlSplit.length -1], FIFO_QUEUE_NAME +".fifo");
	}
	
	@Test
	public void testCreateDeadLetterQueue() {
		SQSProvider.builder().createQueue(SQSTypeQueue.AWS_SQS_DEAD_LETTER, DEAD_LETTER_QUEUE_NAME);
		String urlQueue = SQSProvider.builder().getUrlQueue(DEAD_LETTER_QUEUE_NAME);
		String[] urlSplit = urlQueue.split("/");
		
		String urlQueueDeadLetter = SQSProvider.builder().getUrlQueue(DEAD_LETTER_QUEUE_NAME +"-dead-letter");
		String[] urlSplitDeadLetter = urlQueueDeadLetter.split("/");
		
		assertEquals(urlSplit[urlSplit.length -1], DEAD_LETTER_QUEUE_NAME);
		assertEquals(urlSplitDeadLetter[urlSplitDeadLetter.length -1], DEAD_LETTER_QUEUE_NAME +"-dead-letter");
	}

}
