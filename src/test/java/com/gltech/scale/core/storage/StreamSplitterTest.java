package com.gltech.scale.core.storage;

import static junit.framework.Assert.*;

import com.gltech.scale.core.storage.StreamSplitter;
import com.ning.compress.lzf.LZFDecoder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class StreamSplitterTest
{
	@Test
	public void testSplit() throws Exception
	{
		byte[] origData = getDataByMb(1);
		String origStrData = new String(origData);
		ByteArrayInputStream bais = new ByteArrayInputStream(origData);
		StreamSplitter streamSplitter = new StreamSplitter(bais, 1024 * 10);


		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		while (streamSplitter.hasNext())
		{
			StreamSplitter.StreamPart streamPart = streamSplitter.next();
			InputStream inputStream = streamPart.getInputStream();

			byte[] buffer = new byte[1024];
			int bytesRead;
			while ((bytesRead = inputStream.read(buffer)) != -1)
			{
				baos.write(buffer, 0, bytesRead);
			}
		}

		byte[] data = LZFDecoder.decode(baos.toByteArray());
		String strData = new String(data);
		assertEquals(origData.length, data.length);
		assertEquals(origStrData.substring(0, 10), strData.substring(0, 10));
		assertEquals(origStrData.substring(origStrData.length() - 60), strData.substring(strData.length() - 60));
	}

	static public byte[] getDataByMb(int mb)
	{
		StringBuilder strData = new StringBuilder();

		int counter = 0;
		while (strData.length() < mb * 1024 * 1024)
		{
			if (strData.length() > 0)
			{
				strData.append(",");
			}

			strData.append(counter++);
		}

		return strData.toString().getBytes();
//		byte[] data = strData.toString().getBytes();
//		byte[] uniform = new byte[mb * 1024 * 1024];
//		System.arraycopy(data, 0, uniform, 0, mb * 1024 * 1024);
//		return uniform;
	}

	@Test
	public void test50PercentCompression() throws Exception
	{
		ByteArrayInputStream bais = new ByteArrayInputStream(getDataByMb(5));
		StreamSplitter streamSplitter = new StreamSplitter(bais, 5 * 1024 * 2014);
		StreamSplitter.StreamPart streamPart = streamSplitter.next();
		assertTrue(streamPart.getSize() < ((5 * 1024 * 1024) / 2));
	}
}
