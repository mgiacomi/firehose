package com.gltech.scale.core.websocket;

import com.dyuproject.protostuff.JsonIOUtil;
import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class SocketIO
{
	private Schema<SocketRequest> requestSchema = RuntimeSchema.getSchema(SocketRequest.class);

	public byte[] toBytes(SocketRequest socketRequest)
	{
		if (socketRequest != null)
		{
			LinkedBuffer linkedBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
			return ProtostuffIOUtil.toByteArray(socketRequest, requestSchema, linkedBuffer);
		}
		return new byte[0];
	}

	public String toJson(SocketRequest socketRequest)
	{
		if (socketRequest != null)
		{
			return new String(JsonIOUtil.toByteArray(socketRequest, requestSchema, false));
		}
		return "{}";
	}

	public SocketRequest toSocketRequest(byte[] bytes)
	{
		SocketRequest socketRequest = new SocketRequest(0, new byte[0]);
		ProtostuffIOUtil.mergeFrom(bytes, socketRequest, requestSchema);
		return socketRequest;
	}

	public SocketRequest toSocketRequest(InputStream inputStream)
	{
		try
		{
			SocketRequest socketRequest = new SocketRequest(0, new byte[0]);
			ProtostuffIOUtil.mergeFrom(inputStream, socketRequest, requestSchema);
			return socketRequest;
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public SocketRequest toSocketRequest(String json)
	{
		try
		{
			SocketRequest socketRequest = new SocketRequest(0, new byte[0]);
			JsonIOUtil.mergeFrom(json.getBytes(), socketRequest, requestSchema, false);
			return socketRequest;
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}
}
