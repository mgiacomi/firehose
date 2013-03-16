package com;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.ExplicitIdStrategy;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.gltech.scale.core.model.Message;
import com.ning.compress.lzf.LZFEncoder;
import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;
import org.joda.time.DateTime;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import java.io.*;
import java.util.UUID;

public class PerformanceRegression
{
private static final long MegaBytes = 1024L * 1024L;

	@Test
	public void writeProtoStuffToFile() throws Exception
	{
		ExplicitIdStrategy.Registry registry = new ExplicitIdStrategy.Registry();
		registry.registerPojo(Message.class, 1);
		Schema<Message> schema = RuntimeSchema.getSchema(Message.class);

		int items = 100000;
		OutputStream fos = new LZFOutputStream(new FileOutputStream(new File("stream.protostuff")));
		LinkedBuffer linkedBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
		long timer = System.currentTimeMillis();

		for (int i = 0; i < items; i++)
		{
			String testString = i + "asdf123asdf123asdf123asdf132asdf132asdf132a1sdf321asdf312adsf31asdf312adsf31asdf31asd";
			ProtostuffIOUtil.writeDelimitedTo(fos, new Message(MediaType.APPLICATION_JSON_TYPE, testString.getBytes()), schema, linkedBuffer);
			linkedBuffer.clear();
		}


		fos.close();
		System.out.println(items + " items to protostuff binary in " + (System.currentTimeMillis() - timer) + "ms");
	}

	@Test
	public void writeProtoStuffToFile2() throws Exception
	{
		ExplicitIdStrategy.Registry registry = new ExplicitIdStrategy.Registry();
		registry.registerPojo(Message.class, 1);
		Schema<Message> schema = RuntimeSchema.getSchema(Message.class);

		int items = 100000;
		ByteArrayOutputStream fos = new ByteArrayOutputStream();
		LinkedBuffer linkedBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
		long timer = System.currentTimeMillis();

		for (int i = 0; i < items; i++)
		{
			String testString = i + "asdf123asdf123asdf123asdf132asdf132asdf132a1sdf321asdf312adsf31asdf312adsf31asdf31asd";
			byte[] data = ProtostuffIOUtil.toByteArray(new Message(MediaType.APPLICATION_JSON_TYPE, testString.getBytes()), schema, linkedBuffer);
			fos.write(LZFEncoder.encode(data));
			linkedBuffer.clear();
		}


		fos.close();
		System.out.println(fos.size() / MegaBytes + " mb, " + items + " items to protostuff2 binary in " + (System.currentTimeMillis() - timer) + "ms");
	}

	@Test
	public void writeProtoStuffToFile3() throws Exception
	{
		ExplicitIdStrategy.Registry registry = new ExplicitIdStrategy.Registry();
		registry.registerPojo(Message.class, 1);
		Schema<Message> schema = RuntimeSchema.getSchema(Message.class);

		int items = 100000;
		ByteArrayOutputStream fos = new ByteArrayOutputStream();
		LinkedBuffer linkedBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
		long timer = System.currentTimeMillis();

		for (int i = 0; i < items; i++)
		{
			String testString = i + "asdf123asdf123asdf123asdf132asdf132asdf132a1sdf321asdf312adsf31asdf312adsf31asdf31asd";
			byte[] data = ProtostuffIOUtil.toByteArray(new Message(MediaType.APPLICATION_JSON_TYPE, testString.getBytes()), schema, linkedBuffer);
			fos.write(data);
			linkedBuffer.clear();
		}


		fos.close();
		System.out.println(fos.size() / MegaBytes + " mb, " + items + " items to protostuff3 binary in " + (System.currentTimeMillis() - timer) + "ms");
	}

	/*
	@Test
	public void fillToDeathObject() throws Exception
	{
		List<MessageBuf> messages = new ArrayList<>();

		int counter = 0;
		int loopCount = 0;
		while (true)
		{
			String testString = counter + "asdf123asdf123asdf123asdf132asdf132asdf132a1sdf321asdf312adsf31asdf312adsf31asdf31asd";
			messages.add(new MessageBuf(UUID.randomUUID().toString(), DateTime.now().getMillis(), testString.getBytes()));

			if (loopCount == 100)
			{
				System.out.println(counter + ",");
				loopCount = 0;
			}
			counter++;
			loopCount++;
		}
	}

	@Test
	public void fillToDeathByteArray() throws Exception
	{
		ExplicitIdStrategy.Registry registry = new ExplicitIdStrategy.Registry();
		registry.registerPojo(MessageBuf.class, 1);
		Schema<MessageBuf> schema = RuntimeSchema.getSchema(MessageBuf.class);

		LinkedBuffer linkedBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);

		List<byte[]> bytes = new ArrayList<>();

		int counter = 0;
		int loopCount = 0;
		while (true)
		{
			String testString = counter + "asdf123asdf123asdf123asdf132asdf132asdf132a1sdf321asdf312adsf31asdf312adsf31asdf31asd";
			bytes.add(ProtostuffIOUtil.toByteArray(new MessageBuf(UUID.randomUUID().toString(), DateTime.now().getMillis(), testString.getBytes()), schema, linkedBuffer));
			linkedBuffer.clear();

			if (loopCount == 100)
			{
				System.out.println(counter + ",");
				loopCount = 0;
			}
			counter++;
			loopCount++;
		}
	}
	*/

	@Test
	public void readProtoStuffToFile() throws Exception
	{
		ExplicitIdStrategy.Registry registry = new ExplicitIdStrategy.Registry();
		registry.registerPojo(Message.class, 1);
		Schema<Message> schema = RuntimeSchema.getSchema(Message.class);

		InputStream fis = new LZFInputStream(new FileInputStream(new File("stream.protostuff")));

		int counter = 0;
		Message message = new Message();
		Message lastMessage = null;
		try
		{
			while (true)
			{
				ProtostuffIOUtil.mergeDelimitedFrom(fis, message, schema);
				lastMessage = message;
				message = new Message();
				//System.out.println(new String(message.getPayload()));
				counter++;
			}
		}
		catch (EOFException e)
		{
			// no prob just end of file.
			System.out.println("yeah got the end " + counter);
		}

		System.out.println(lastMessage.getUuid() + " : " + lastMessage.getReceived_at() + " : " + new String(lastMessage.getPayload()));

		fis.close();
	}
}
