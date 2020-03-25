package com.queuefactory.provider.rabbitmq;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class RabbitMQUtil {
	
	public static Object deserialize(byte[] message) throws IOException, ClassNotFoundException {
		ByteArrayInputStream in = new ByteArrayInputStream(message);
		ObjectInputStream is = new ObjectInputStream(in);
		return is.readObject();
	}

}
