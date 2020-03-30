package com.queuefactory.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public class Util {
	
	public static String serializeToBase64(Object object) throws IOException {
        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = null;
        try {
            objectStream = new ObjectOutputStream(byteArrayStream);
            objectStream.writeObject(object);
            return new String(Base64.getEncoder().encode(byteArrayStream.toByteArray()));
        } finally {
            if (objectStream != null)
                objectStream.close();
        }
    }

	public static Object deserializeFromBase64(String data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayStream = new ByteArrayInputStream(Base64.getDecoder().decode(data));

        ObjectInputStream objectStream = null;
        try {
            objectStream = new ObjectInputStream(byteArrayStream);
            return objectStream.readObject();
        } finally {
            if (objectStream != null)
                objectStream.close();
        }
    }
	
	public static byte[] getByteArray(Object obj) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(out);
		os.writeObject(obj);
		return out.toByteArray();
	}
	
	public static Object deserialize(byte[] message) throws IOException, ClassNotFoundException {
		ByteArrayInputStream in = new ByteArrayInputStream(message);
		ObjectInputStream is = new ObjectInputStream(in);
		return is.readObject();
	}

}
