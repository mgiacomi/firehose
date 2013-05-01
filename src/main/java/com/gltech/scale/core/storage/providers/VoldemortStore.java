package com.gltech.scale.core.storage.providers;

import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.stats.AvgStatOverTime;
import com.gltech.scale.core.stats.CountStatOverTime;
import com.gltech.scale.core.stats.StatsManager;
import com.gltech.scale.core.storage.KeyAlreadyExistsException;
import com.gltech.scale.core.storage.Storage;
import com.gltech.scale.core.model.ModelIO;
import com.gltech.scale.util.VoldemortClient;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import voldemort.client.StoreClient;
import voldemort.versioning.Versioned;

import java.io.*;

public class VoldemortStore implements Storage
{
	private static final Logger logger = LoggerFactory.getLogger(VoldemortStore.class);
	private StoreClient<String, byte[]> channelClient;
	private AvgStatOverTime keyReadTimeStat;
	private AvgStatOverTime keyReadSizeStat;
	private CountStatOverTime bytesReadStat;
	private AvgStatOverTime keyWrittenTimeStat;
	private AvgStatOverTime keyWrittenSizeStat;
	private CountStatOverTime bytesWrittenStat;
	private ModelIO modelIO;

	@Inject
	public VoldemortStore(ModelIO modelIO, StatsManager statsManager)
	{
		this.modelIO = modelIO;
		this.channelClient = VoldemortClient.createFactory().getStoreClient("ChannelStore");

		String groupName = "Storage";
		keyWrittenTimeStat = statsManager.createAvgStat(groupName, "KeysWritten_AvgTime", "milliseconds");
		keyWrittenTimeStat.activateCountStat("KeysWritten_Count", "keys");
		keyWrittenSizeStat = statsManager.createAvgStat(groupName, "KeysWritten_Size", "kb");
		bytesWrittenStat = statsManager.createCountStat(groupName, "BytesWritten_Count", "bytes");
		keyReadTimeStat = statsManager.createAvgStat(groupName, "KeysRead_AvgTime", "milliseconds");
		keyReadTimeStat.activateCountStat("KeysRead_Count", "keys");
		keyReadSizeStat = statsManager.createAvgStat(groupName, "KeyRead_Size", "kb");
		bytesReadStat = statsManager.createCountStat(groupName, "BytesRead_Count", "bytes");
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
			keyWrittenTimeStat.startTimer();
			byte[] bytes = IOUtils.toByteArray(inputStream);
			messageClient.put(channelMetaData.getName() + "|" + id, bytes);
			keyWrittenTimeStat.stopTimer();
			keyWrittenSizeStat.add(bytes.length / Defaults.KBytes);
			bytesWrittenStat.add(bytes.length);
			logger.info("Wrote {} bytes for key {}", bytes.length, channelMetaData.getName() + "|" + id);
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
			keyReadTimeStat.startTimer();
			Versioned<byte[]> versioned = messageClient.get(channelMetaData.getName() + "|" + id);

			if (versioned != null && versioned.getValue().length > 0)
			{
				outputStream.write(versioned.getValue());
				keyReadSizeStat.add(versioned.getValue().length / Defaults.KBytes);
				bytesReadStat.add(versioned.getValue().length);
			}
			keyReadTimeStat.stopTimer();
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	@Override
	public void putBytes(ChannelMetaData channelMetaData, String id, byte[] data)
	{
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		putMessages(channelMetaData, id, bais);
	}

	@Override
	public byte[] getBytes(ChannelMetaData channelMetaData, String id)
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		getMessages(channelMetaData, id, baos);
		return baos.toByteArray();
	}
}
