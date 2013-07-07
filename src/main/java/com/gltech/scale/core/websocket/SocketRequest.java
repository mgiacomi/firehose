package com.gltech.scale.core.websocket;

import com.dyuproject.protostuff.Tag;

import java.util.HashMap;
import java.util.Map;

public class SocketRequest
{
	@Tag(1)
	private final int id;
	@Tag(2)
	private Map<String, String> headers = new HashMap<>();
	@Tag(3)
	private final byte[] data;

	public SocketRequest(int id, byte[] data)
	{
		this.id = id;
		this.data = data;
	}

	public void addHeader(String key, String value)
	{
		headers.put(key, value);
	}

	public int getId()
	{
		return id;
	}

	public String getHeader(String key)
	{
		return headers.get(key);
	}

	public Map<String, String> getHeaders()
	{
		return headers;
	}

	public byte[] getData()
	{
		return data;
	}
}
