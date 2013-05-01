package com.gltech.scale.core.storage.providers;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.stats.AvgStatOverTime;
import com.gltech.scale.core.stats.CountStatOverTime;
import com.gltech.scale.core.stats.StatsManager;
import com.gltech.scale.core.stats.StatsThreadPoolExecutor;
import com.gltech.scale.core.storage.StreamSplitter;
import com.gltech.scale.core.model.ModelIO;
import com.google.common.base.Throwables;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.Storage;
import com.gltech.scale.util.Props;
import com.google.inject.Inject;
import com.ning.compress.lzf.LZFInputStream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class AwsS3Store implements Storage
{
	private static final Logger logger = LoggerFactory.getLogger(AwsS3Store.class);
	private Props props = Props.getProps();
	private AmazonS3 s3Client;
	private String s3BucketName;
	private AvgStatOverTime keyReadTimeStat;
	private AvgStatOverTime keyReadSizeStat;
	private CountStatOverTime bytesReadStat;
	private AvgStatOverTime keyWrittenTimeStat;
	private AvgStatOverTime keyWrittenSizeStat;
	private CountStatOverTime bytesWrittenStat;
	private final ThreadPoolExecutor threadPoolExecutor;
	private final Semaphore semaphore;
	private ModelIO modelIO;

	@Inject
	public AwsS3Store(ModelIO modelIO, StatsManager statsManager)
	{
		this.modelIO = modelIO;
		s3BucketName = props.get("s3BucketName", "gltech");
		String accessKey = props.get("accessKey", "AKIAIMCO3L5X25HGADAQ");
		String secretKey = props.get("secretKey", "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");
		s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));

		int activeUploads = props.get("storage.s3.concurrent_writes", Defaults.CONCURRENT_STORE_WRITES);

		String groupName = "Storage";
		keyWrittenTimeStat = statsManager.createAvgStat(groupName, "KeysWritten_AvgTime", "milliseconds");
		keyWrittenTimeStat.activateCountStat("KeysWritten_Count", "keys");
		keyWrittenSizeStat = statsManager.createAvgStat(groupName, "KeysWritten_Size", "kb");
		bytesWrittenStat = statsManager.createCountStat(groupName, "BytesWritten_Count", "bytes");
		keyReadTimeStat = statsManager.createAvgStat(groupName, "KeysRead_AvgTime", "milliseconds");
		keyReadTimeStat.activateCountStat("KeysRead_Count", "keys");
		keyReadSizeStat = statsManager.createAvgStat(groupName, "KeyRead_Size", "kb");
		bytesReadStat = statsManager.createCountStat(groupName, "BytesRead_Count", "bytes");

		TransferQueue<Runnable> queue = new LinkedTransferQueue<>();
		threadPoolExecutor = new StatsThreadPoolExecutor(activeUploads, activeUploads, 1, TimeUnit.MINUTES, queue, new S3UploadThreadFactory(), keyWrittenTimeStat);
		semaphore = new Semaphore(activeUploads);
	}

	@Override
	public void putChannelMetaData(ChannelMetaData channelMetaData)
	{
		String key = channelMetaData.getName();

		byte[] jsonData = modelIO.toJsonBytes(channelMetaData);

		ByteArrayInputStream bais = new ByteArrayInputStream(jsonData);

		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(jsonData.length);

		s3Client.putObject(s3BucketName, key, bais, metadata);
	}

	@Override
	public ChannelMetaData getChannelMetaData(String channelName)
	{
		S3Object s3Object = null;

		try
		{
			s3Object = s3Client.getObject(s3BucketName, channelName);
		}
		catch (AmazonServiceException e)
		{
			if ("NoSuchKey".equalsIgnoreCase(e.getErrorCode()) || e.getStatusCode() == 404)
			{
				return null;
			}
		}

		try
		{
			byte[] data = IOUtils.toByteArray(s3Object.getObjectContent());
			return modelIO.toChannelMetaData(new String(data));
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	@Override
	public void getMessages(ChannelMetaData channelMetaData, String id, OutputStream outputStream)
	{
		String key = keyNameWithUniquePrefix(channelMetaData.getName(), id);
		S3Object s3Object = null;

		keyReadTimeStat.startTimer();
		try
		{
			s3Object = s3Client.getObject(s3BucketName, key);
		}
		catch (AmazonServiceException e)
		{
			if ("NoSuchKey".equalsIgnoreCase(e.getErrorCode()) || e.getStatusCode() == 404)
			{
				return;
			}
		}

		try
		{
			int bytesWritten = IOUtils.copy(new LZFInputStream(s3Object.getObjectContent()), outputStream);
			keyReadTimeStat.stopTimer();
			keyReadSizeStat.add(bytesWritten / Defaults.KBytes);
			bytesReadStat.add(bytesWritten);
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	@Override
	public void putMessages(ChannelMetaData channelMetaData, String id, InputStream inputStream)
	{
		String key = keyNameWithUniquePrefix(channelMetaData.getName(), id);

		// Set part size to 5 MB.
		int partSize = 5 * Defaults.MEGABYTES;

		// Create a list of UploadPartResponse objects. You get one of these for each part upload.
		List<PartETag> partETags = new ArrayList<>();

		// Step 1: Initialize.
		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(s3BucketName, key);
		InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

		try
		{
			StreamSplitter streamSplitter = new StreamSplitter(inputStream, partSize);

			// Step 2a: Submit Upload parts.
			int part = 1;
			List<Future<PartETag>> uploads = new ArrayList<>();

			while (streamSplitter.hasNext())
			{
				semaphore.acquire();
				StreamSplitter.StreamPart streamPart = streamSplitter.next();

				try
				{
					Callable<PartETag> uploadPart = new PartUploader(streamPart, key, part, initResponse);
					uploads.add(threadPoolExecutor.submit(uploadPart));
					keyWrittenSizeStat.add(streamPart.getSize() / Defaults.KBytes);
					bytesWrittenStat.add(streamPart.getSize());
					part++;
				}
				finally
				{
					semaphore.release();
				}
			}

			// Step 2b: Wait for Upload parts to complete.
			for (Future<PartETag> upload : uploads)
			{
				partETags.add(upload.get());
			}

			// Step 3: complete upload.
			CompleteMultipartUploadRequest compRequest = new
					CompleteMultipartUploadRequest(
					s3BucketName,
					key,
					initResponse.getUploadId(),
					partETags);

			s3Client.completeMultipartUpload(compRequest);
		}
		catch (Exception e)
		{
			s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(s3BucketName, key, initResponse.getUploadId()));
			logger.error("Failed to upload key to S3.  key=" + key, e);
			throw Throwables.propagate(e);
		}
	}

	@Override
	public byte[] getBytes(ChannelMetaData channelMetaData, String id)
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		getMessages(channelMetaData, id, baos);
		return baos.toByteArray();
	}

	@Override
	public void putBytes(ChannelMetaData channelMetaData, String id, byte[] data)
	{
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		putMessages(channelMetaData, id, bais);

	}

	private String keyNameWithUniquePrefix(String channelName, String id)
	{
		try
		{
			MessageDigest md = MessageDigest.getInstance("MD5");
			String key = channelName + "|" + id;
			md.update(key.getBytes(), 0, key.length());
			String md5 = new BigInteger(1, md.digest()).toString(16);
			return md5.substring(0, 2) + "|" + channelName + "|" + id;
		}
		catch (NoSuchAlgorithmException e)
		{
			throw Throwables.propagate(e);
		}
	}

	private class PartUploader implements Callable<PartETag>
	{
		private StreamSplitter.StreamPart streamPart;
		private InitiateMultipartUploadResult initResponse;
		private String key;
		private int part;

		private PartUploader(StreamSplitter.StreamPart streamPart, String key, int part, InitiateMultipartUploadResult initResponse)
		{
			this.streamPart = streamPart;
			this.initResponse = initResponse;
			this.key = key;
			this.part = part;
		}

		public PartETag call() throws Exception
		{
			logger.debug("Start S3 Part Upload key={}, part={}", key, part);

			// Create request to upload a part.
			UploadPartRequest uploadRequest = new UploadPartRequest()
					.withBucketName(s3BucketName).withKey(key)
					.withUploadId(initResponse.getUploadId()).withPartNumber(part)
					.withInputStream(streamPart.getInputStream())
					.withPartSize(streamPart.getSize());

			logger.debug("Completed S3 Part Upload key={}, part={}, size={}mb", key, part, streamPart.getSize() / Defaults.MEGABYTES);

			// Upload part and add response to our list.
			return s3Client.uploadPart(uploadRequest).getPartETag();
		}
	}

	private class S3UploadThreadFactory implements ThreadFactory
	{
		public Thread newThread(Runnable r)
		{
			return new Thread(r, "S3Uploader");
		}
	}
}
