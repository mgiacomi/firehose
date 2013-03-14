package com.gltech.scale.core.model;

import com.dyuproject.protostuff.Tag;

public class MessageBuf
{
	//this.uuid = UUID.randomUUID().toString();
	//this.received_at = DateTime.now().getMillis();

	@Tag(2)
	private final long received_at;
	@Tag(3)
	private final byte[] payload;
	@Tag(4)
	private final String uuid;
	@Tag(5)
	private final boolean stored;

	public MessageBuf() {
		// This method is only to init class for de-serialization
		this.received_at = 0;
		this.payload = null;
		this.uuid = null;
		this.stored = false;
	}

	public MessageBuf(String uuid, long received_at)
	{
		this.uuid = uuid;
		this.received_at = received_at;
		this.payload = new byte[0];
		this.stored = true;
	}

	public MessageBuf(String uuid, long received_at, byte[] payload)
	{
		this.uuid = uuid;
		this.received_at = received_at;
		this.payload = payload;
		this.stored = false;
	}

	public long getReceived_at()
	{
		return received_at;
	}

	public byte[] getPayload()
	{
		return payload;
	}

	public String getUuid()
	{
		return uuid;
	}

	public boolean isStored()
	{
		return stored;
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		MessageBuf that = (MessageBuf) o;

		if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) return false;

		return true;
	}

	public int hashCode()
	{
		return  uuid != null ? uuid.hashCode() : 0;
	}

	public int compareTo(MessageBuf o)
	{
		int compare = ((Long)this.getReceived_at()).compareTo(o.getReceived_at());

		if (compare != 0)
		{
			return compare;
		}

		return this.getUuid().compareTo(o.getUuid());
	}
}
