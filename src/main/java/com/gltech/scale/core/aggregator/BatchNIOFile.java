package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.util.Props;
import com.google.common.base.Throwables;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

public class BatchNIOFile implements Batch
{
	private static final Logger logger = LoggerFactory.getLogger(BatchNIOFile.class);
	private DateTime nearestPeriodCeiling;
	private DateTime firstMessageTime;
	private DateTime lastMessageTime = DateTime.now();
	private AtomicLong messages = new AtomicLong(0);
	private AtomicLong bytes = new AtomicLong(0);
	private ChannelMetaData channelMetaData;
	private final File file;
	private final FileOutputStream fileOutputStream;
	private final FileChannel outputChannel;

	public BatchNIOFile(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling)
	{
		this.channelMetaData = channelMetaData;
		this.nearestPeriodCeiling = nearestPeriodCeiling;
		Props props = Props.getProps();

		File directory = new File(props.get("channel_file_dir", Defaults.CHANNEL_FILE_DIR));
		if (!directory.exists())
		{
			if (directory.mkdirs())
			{
				logger.info("Created File Channel Directory: " + directory.getAbsolutePath());
			}
		}

		try
		{
			File file = new File(directory, channelMetaData.getName() + "_" + nearestPeriodCeiling.toString("YYYY-MM-dd-HH-mm-ss"));

			int fileCounter = 0;
			while(file.exists()) {
				file = new File(directory, channelMetaData.getName() + "_" + nearestPeriodCeiling.toString("YYYY-MM-dd-HH-mm-ss") +"."+ fileCounter++);
			}

			this.file = file;
			this.fileOutputStream = new FileOutputStream(file);
			this.outputChannel = fileOutputStream.getChannel();
		}
		catch (FileNotFoundException e)
		{
			logger.error("Failed to create an NIO file channel.", e);
			throw Throwables.propagate(e);
		}
	}

	private byte[] append(byte value, byte[] origArray)
	{
		byte[] newArray = new byte[origArray.length + 1];
		System.arraycopy(origArray, 0, newArray, 0, origArray.length);
		newArray[origArray.length] = value;
		return newArray;
	}

	/**
	 * Encode and write a varint.  {@code value} is treated as
	 * unsigned, so it won't be sign-extended if negative.
	 */
	byte[] getRawVarint32(int value)
	{
		byte[] rawVarint32 = new byte[]{};
		while (true)
		{
			if ((value & ~0x7F) == 0)
			{
				return append((byte) value, rawVarint32);
			}
			else
			{
				rawVarint32 = append((byte) ((value & 0x7F) | 0x80), rawVarint32);
				value >>>= 7;
			}
		}
	}

	public void addMessage(byte[] message)
	{
		byte[] header = getRawVarint32(message.length);
		ByteBuffer buffer = ByteBuffer.allocateDirect(header.length + message.length);
		buffer.clear();
		buffer.put(header);
		buffer.put(message);
		buffer.flip();

		try
		{
			outputChannel.write(buffer);
			lastMessageTime = DateTime.now();
			messages.incrementAndGet();
			bytes.addAndGet(message.length);

			if (firstMessageTime == null)
			{
				firstMessageTime = DateTime.now();
			}
		}
		catch (IOException e)
		{
			logger.error("Failed to write to file channel.", e);
			throw Throwables.propagate(e);
		}
	}

	@Override
	public long writeMessages(OutputStream outputStream)
	{
		try
		{
			// Close the file for writing
			fileOutputStream.close();

			// Read the file
			FileInputStream fileInputStream = new FileInputStream(file);
			FileChannel inputChannel = fileInputStream.getChannel();

			int bufferSize = 8 * Defaults.KBytes;
			ByteBuffer bb = ByteBuffer.allocate(Defaults.MBytes);
			byte[] barray = new byte[bufferSize];
			int nRead, nGet;
			while ((nRead = inputChannel.read(bb)) != -1)
			{
				if (nRead == 0)
				{
					continue;
				}
				bb.position(0);
				bb.limit(nRead);
				while (bb.hasRemaining())
				{
					nGet = Math.min(bb.remaining(), bufferSize);
					bb.get(barray, 0, nGet);
					outputStream.write(barray, 0, nGet);
				}
				bb.clear();
			}

			fileInputStream.close();

			return messages.get();
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	@Override
	public void clear()
	{
		try
		{
			// Close the file for writing and delete it.
			fileOutputStream.close();
			file.delete();
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	@Override
	public ChannelMetaData getChannelMetaData()
	{
		return channelMetaData;
	}

	@Override
	public long getMessages()
	{
		return messages.get();
	}

	@Override
	public long getBytes()
	{
		return bytes.get();
	}

	@Override
	public DateTime getLastMessageTime()
	{
		return lastMessageTime;
	}

	@Override
	public DateTime getFirstMessageTime()
	{
		return firstMessageTime;
	}

	@Override
	public BatchMetaData getMetaData()
	{
		return new BatchMetaData(nearestPeriodCeiling, messages.get(), bytes.get(), channelMetaData);
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		BatchNIOFile that = (BatchNIOFile) o;

		if (channelMetaData != null ? !channelMetaData.equals(that.channelMetaData) : that.channelMetaData != null)
			return false;
		if (nearestPeriodCeiling != null ? !nearestPeriodCeiling.equals(that.nearestPeriodCeiling) : that.nearestPeriodCeiling != null)
			return false;

		return true;
	}

	public int hashCode()
	{
		int result = nearestPeriodCeiling != null ? nearestPeriodCeiling.hashCode() : 0;
		result = 31 * result + (channelMetaData != null ? channelMetaData.hashCode() : 0);
		return result;
	}

}
