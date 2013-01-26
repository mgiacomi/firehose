package com.gltech.scale.core.storage.stream;

import com.google.common.base.Throwables;
import com.ning.compress.lzf.LZFCompressingInputStream;

import java.io.*;

public class StreamSplitter
{
	private final int partSize;
	private final InputStream inputStream;
	private boolean hasNext = true;

	public StreamSplitter(InputStream inputStream, int partSize)
	{
		this.inputStream = new LZFCompressingInputStream(inputStream);
		this.partSize = partSize;
	}

	public boolean hasNext()
	{
		return hasNext;
	}

	public StreamPart next()
	{
		try
		{
			byte[] buffer = new byte[partSize];
			int bytesRead = 0;
			int data = -1;

			while (bytesRead < partSize)
			{
				data = inputStream.read();

				if (data == -1)
				{
					break;
				}

				buffer[bytesRead] = (byte) data;
				bytesRead++;
			}

			if (data == -1)
			{
				hasNext = false;

				byte[] smallerBuffer = new byte[bytesRead];
				System.arraycopy(buffer, 0, smallerBuffer, 0, bytesRead);

				return new StreamPart(smallerBuffer);
			}
			else
			{
				return new StreamPart(buffer);
			}
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public class StreamPart
	{
		private final byte[] data;

		public StreamPart(byte[] data)
		{
			this.data = data;
		}

		public byte[] getData()
		{
			return data;
		}

		public long getSize()
		{
			return data.length;
		}

		public InputStream getInputStream()
		{
			return new ByteArrayInputStream(data);
		}
	}
}

