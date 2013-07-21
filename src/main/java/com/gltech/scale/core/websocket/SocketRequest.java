package com.gltech.scale.core.websocket;

import org.joda.time.DateTime;

import java.nio.ByteBuffer;

public class SocketRequest
{
	public static final int PRIMARY = 0;
	public static final int BACKUP = 1;

	private final int id;
	private final int mode;
	private final String channelName;
	private final DateTime period;
	private final byte[] data;

	/*
		The goal of this class is to function as a binary record for a request.  It is a four part messages:

		-  4 bytes for an id
		-  1 byte for a mode
		-  25 bytes for channel name
		-  8 bytes for period
		-  +data (the rest for data)

		The goal was to be simple, small, and fast.
	 */

	public SocketRequest(int id, int mode, String channelName, DateTime period, byte[] data)
	{
		if (channelName.length() > 25)
		{
			throw new IllegalArgumentException("Channel name is too big for buffer. Only 25 characters allowed: " + channelName);
		}

		if (mode != PRIMARY && mode != BACKUP)
		{
			throw new IllegalArgumentException("Mode must either be 0 (primary) or 1 (backup). Received mode " + mode);
		}

		this.id = id;
		this.mode = mode;
		this.channelName = channelName;
		this.period = period;
		this.data = data;
	}

	public SocketRequest(byte[] bytes)
	{
		if (bytes.length < 38)
		{
			throw new RuntimeException("Malformed response with only " + bytes.length + " bytes.");
		}

		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		this.id = buffer.getInt();
		this.mode = buffer.get();

		byte[] channelNameData = new byte[25];
		buffer.get(channelNameData);
		this.channelName = new String(channelNameData).trim();

		this.period = new DateTime(buffer.getLong());

		this.data = new byte[bytes.length - 38];
		buffer.get(this.data);
	}

	public ByteBuffer getByteBuffer()
	{
		ByteBuffer buffer = ByteBuffer.allocate(38 + data.length);
		buffer.putInt(id);
		buffer.put((byte) mode);

		byte[] channelNameBytes = channelName.getBytes();
		byte[] channelNameBuffer = new byte[25];
		System.arraycopy(channelNameBytes, 0, channelNameBuffer, 0, channelNameBytes.length);
		buffer.put(channelNameBuffer);

		buffer.putLong(period.getMillis());

		if (data.length > 0)
		{
			buffer.put(data);
		}

		buffer.rewind();

		return buffer;
	}

	public boolean isPrimary()
	{
		return mode == 0;
	}

	public int getId()
	{
		return id;
	}

	public DateTime getPeriod()
	{
		return period;
	}

	public String getChannelName()
	{
		return channelName;
	}

	public byte[] getData()
	{
		return data;
	}

	public String toString()
	{
		return "SocketRequest{" +
				"id=" + id +
				", mode=" + mode +
				", channelName='" + channelName + '\'' +
				'}';
	}
}
