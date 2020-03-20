package com.queuefactory.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.queuefactory.provider.QueueProvider;
import com.queuefactory.provider.TypeQueue;

@SpringBootTest()
public class SQSClientTest {
	
	private static final String STANDARD_QUEUE_NAME = "queue-test1";
	private static final String FIFO_QUEUE_NAME = "queue-test2";
	private static final String DEAD_LETTER_QUEUE_NAME = "queue-test3";
	
	@Test
	public void testCreateStandardQueue() {
		QueueProvider.connect(SQSProvider.builder()).createQueue(TypeQueue.AWS_SQS_STANDARD, STANDARD_QUEUE_NAME);
		String urlQueue = QueueProvider.connect(SQSProvider.builder()).getUrlQueue(STANDARD_QUEUE_NAME);
		String[] urlSplit = urlQueue.split("/");
		assertEquals(urlSplit[urlSplit.length -1], STANDARD_QUEUE_NAME);
	}
	
	@Test
	public void testCreateFifoQueue() {
		QueueProvider.connect(SQSProvider.builder()).createQueue(TypeQueue.AWS_SQS_FIFO, FIFO_QUEUE_NAME);
		String urlQueue = QueueProvider.connect(SQSProvider.builder()).getUrlQueue(FIFO_QUEUE_NAME +".fifo");
		String[] urlSplit = urlQueue.split("/");
		assertEquals(urlSplit[urlSplit.length -1], FIFO_QUEUE_NAME +".fifo");
	}
	
	@Test
	public void testCreateDeadLetterQueue() {
		QueueProvider.connect(SQSProvider.builder()).createQueue(TypeQueue.AWS_SQS_DEAD_LETTER, DEAD_LETTER_QUEUE_NAME);
		String urlQueue = QueueProvider.connect(SQSProvider.builder()).getUrlQueue(DEAD_LETTER_QUEUE_NAME);
		String[] urlSplit = urlQueue.split("/");
		
		String urlQueueDeadLetter = QueueProvider.connect(SQSProvider.builder()).getUrlQueue(DEAD_LETTER_QUEUE_NAME +"-dead-letter");
		String[] urlSplitDeadLetter = urlQueueDeadLetter.split("/");
		
		assertEquals(urlSplit[urlSplit.length -1], DEAD_LETTER_QUEUE_NAME);
		assertEquals(urlSplitDeadLetter[urlSplitDeadLetter.length -1], DEAD_LETTER_QUEUE_NAME +"-dead-letter");
	}

}
