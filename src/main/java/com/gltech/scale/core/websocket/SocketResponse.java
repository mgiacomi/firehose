package com.gltech.scale.core.websocket;


import com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class SocketResponse
{
	/*
		The goal of this class is to function as a binary record for a response.  It is a three part messages:

		-  4 bytes for an id
		-  1 byte for a responseType
		-  +data (the rest for data)

		The goal was to be simple, small, and fast.
	 */

	static public final int ACK = 0;
	static public final int ERROR = -1;
	static public final int DATA = 1;

	private final int id;
	private final int responseType;
	private final byte[] data;

	public SocketResponse(byte[] bytes)
	{
		if (bytes.length < 5)
		{
			throw new RuntimeException("Malformed response with only " + bytes.length + " bytes.");
		}

		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		this.id = buffer.getInt();
		this.responseType = buffer.get();
		this.data = new byte[bytes.length - 5];
		buffer.get(this.data);
	}


	public SocketResponse(int id, int responseType)
	{
		this.id = id;
		this.responseType = responseType;
		this.data = new byte[0];
	}

	public SocketResponse(int id, int responseType, byte[] data)
	{
		this.id = id;
		this.responseType = responseType;
		this.data = data;
	}

	public ByteBuffer getByteBuffer()
	{
		ByteBuffer buffer = ByteBuffer.allocate(5 + data.length);
		buffer.putInt(id);
		buffer.put((byte) responseType);

		if (data.length > 0)
		{
			buffer.put(data);
		}

		buffer.rewind();

		return buffer;
	}

	public boolean isAck()
	{
		return responseType == ACK;
	}

	public boolean isError()
	{
		return responseType == ERROR;
	}

	public boolean isData()
	{
		return responseType == DATA;
	}

	public int getId()
	{
		return id;
	}

	public byte[] getData()
	{
		return data;
	}

	public String toString()
	{
		return "SocketResponse{" +
				"id=" + id +
				", responseType=" + responseType +
				", data=" + data +
				'}';
	}
}
