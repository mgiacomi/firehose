package com.gltech.scale.core.model;

import com.dyuproject.protostuff.JsonIOUtil;
import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.model.Message;
import com.google.common.base.Throwables;

import java.io.IOException;

public class ModelIO
{
	private Schema<Message> messageSchema = RuntimeSchema.getSchema(Message.class);
	private Schema<BatchMetaData> batchMetadataSchema = RuntimeSchema.getSchema(BatchMetaData.class);
	private Schema<ChannelMetaData> channelMetaDataSchema = RuntimeSchema.getSchema(ChannelMetaData.class);

	public byte[] toBytes(Message message)
	{
		if (message != null)
		{
			LinkedBuffer linkedBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
			return ProtostuffIOUtil.toByteArray(message, messageSchema, linkedBuffer);
		}
		return new byte[0];
	}

	public byte[] toBytes(ChannelMetaData channelMetaData)
	{
		if (channelMetaData != null)
		{
			LinkedBuffer linkedBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
			return ProtostuffIOUtil.toByteArray(channelMetaData, channelMetaDataSchema, linkedBuffer);
		}
		return new byte[0];
	}

	public byte[] toBytes(BatchMetaData batchMetaData)
	{
		if (batchMetaData != null)
		{
			LinkedBuffer linkedBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
			return ProtostuffIOUtil.toByteArray(batchMetaData, batchMetadataSchema, linkedBuffer);
		}
		return new byte[0];
	}

	public String toJson(Message message)
	{
		if (message != null)
		{
			return new String(JsonIOUtil.toByteArray(message, messageSchema, false));
		}
		return "{}";
	}

	public byte[] toJsonBytes(ChannelMetaData channelMetaData)
	{
		if (channelMetaData != null)
		{
			return JsonIOUtil.toByteArray(channelMetaData, channelMetaDataSchema, false);
		}
		return new byte[0];
	}

	public String toJson(ChannelMetaData channelMetaData)
	{
		if (channelMetaData != null)
		{
			return new String(JsonIOUtil.toByteArray(channelMetaData, channelMetaDataSchema, false));
		}
		return "{}";
	}

	public String toJson(BatchMetaData batchMetaData)
	{
		if (batchMetaData != null)
		{
			return new String(JsonIOUtil.toByteArray(batchMetaData, batchMetadataSchema, false));
		}
		return "{}";
	}

	public Message toMessage(byte[] bytes)
	{
		Message message = new Message();
		ProtostuffIOUtil.mergeFrom(bytes, message, messageSchema);

		if (message.getUuid() == null)
		{
			throw new IllegalArgumentException("The byte[] supplied was not a Protostuff binary array.");
		}

		return message;
	}

	public Message toMessage(String json)
	{
		try
		{
			Message message = new Message();
			JsonIOUtil.mergeFrom(json.getBytes(), message, messageSchema, false);
			return message;
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public ChannelMetaData toChannelMetaData(byte[] bytes)
	{
		ChannelMetaData channelMetaData = new ChannelMetaData();
		ProtostuffIOUtil.mergeFrom(bytes, channelMetaData, channelMetaDataSchema);
		return channelMetaData;
	}

	public ChannelMetaData toChannelMetaData(String json)
	{
		try
		{
			ChannelMetaData channelMetaData = new ChannelMetaData();
			JsonIOUtil.mergeFrom(json.getBytes(), channelMetaData, channelMetaDataSchema, false);
			return channelMetaData;
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public BatchMetaData toBatchMetaData(byte[] bytes)
	{
		BatchMetaData batchMetaData = new BatchMetaData();
		ProtostuffIOUtil.mergeFrom(bytes, batchMetaData, batchMetadataSchema);
		return batchMetaData;
	}

	public BatchMetaData toBatchMetaData(String json)
	{
		try
		{
			BatchMetaData batchMetaData = new BatchMetaData();
			JsonIOUtil.mergeFrom(json.getBytes(), batchMetaData, batchMetadataSchema, false);
			return batchMetaData;
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}
}
