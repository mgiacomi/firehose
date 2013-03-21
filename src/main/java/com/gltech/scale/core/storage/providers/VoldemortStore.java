package com.gltech.scale.core.storage.providers;

import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.KeyAlreadyExistsException;
import com.gltech.scale.core.storage.Storage;
import com.gltech.scale.util.ModelIO;
import com.gltech.scale.util.VoldemortClient;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import voldemort.client.StoreClient;
import voldemort.versioning.Versioned;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class VoldemortStore implements Storage
{
	private static final Logger logger = LoggerFactory.getLogger(VoldemortStore.class);
	private StoreClient<String, byte[]> channelClient;
	private ModelIO modelIO;

	@Inject
	public VoldemortStore(ModelIO modelIO)
	{
		this.modelIO = modelIO;
		this.channelClient = VoldemortClient.createFactory().getStoreClient("ChannelStore");
	}

	@Override
	public void putChannelMetaData(final ChannelMetaData channelMetaData)
	{
		Versioned<byte[]> versioned = channelClient.get(channelMetaData.getName());

		if (versioned != null)
		{
			throw new KeyAlreadyExistsException("Channel already exists " + channelMetaData.getName());
		}

		channelClient.put(channelMetaData.getName(), modelIO.toBytes(channelMetaData));
	}

	@Override
	public ChannelMetaData getChannelMetaData(String channelName)
	{
		Versioned<byte[]> versioned = channelClient.get(channelName);

		if (versioned != null)
		{
			return modelIO.toChannelMetaData(versioned.getValue());
		}

		return null;
	}

	@Override
	public void putMessages(ChannelMetaData channelMetaData, String id, InputStream inputStream)
	{
		StoreClient<String, byte[]> messageClient = VoldemortClient.createFactory().getStoreClient(channelMetaData.getTtl() + "Store");

		try
		{
			byte[] bytes = IOUtils.toByteArray(inputStream);
			messageClient.put(channelMetaData.getName() +"|"+id, bytes);
			logger.info("Wrote {} bytes for key {}", bytes.length, channelMetaData.getName() +"|"+id);
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	@Override
	public void getMessages(ChannelMetaData channelMetaData, String id, OutputStream outputStream)
	{
		StoreClient<String, byte[]> messageClient = VoldemortClient.createFactory().getStoreClient(channelMetaData.getTtl() + "Store");

		try
		{
			Versioned<byte[]> versioned = messageClient.get(channelMetaData.getName() +"|"+id);

			if (versioned != null && versioned.getValue().length > 0)
			{
				outputStream.write(versioned.getValue());
			}
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	@Override
	public void putBytes(ChannelMetaData channelMetaData, String id, byte[] data)
	{
		StoreClient<String, byte[]> messageClient = VoldemortClient.createFactory().getStoreClient(channelMetaData.getTtl() + "Store");
		messageClient.put(channelMetaData.getName() +"|"+id, data);
	}

	@Override
	public byte[] getBytes(ChannelMetaData channelMetaData, String id)
	{
		StoreClient<String, byte[]> messageClient = VoldemortClient.createFactory().getStoreClient(channelMetaData.getTtl() + "Store");
		return messageClient.get(channelMetaData.getName() +"|"+id).getValue();
	}
}
