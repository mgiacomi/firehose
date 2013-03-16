package com.gltech.scale.core.storage.providers;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.google.common.base.Throwables;
import com.gltech.scale.ganglia.Timer;
import com.gltech.scale.ganglia.TimerThreadPoolExecutor;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.Storage;
import com.gltech.scale.util.Props;
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
import java.util.Map;
import java.util.concurrent.*;

public class AwsS3Storage implements Storage
{
	private static final Logger logger = LoggerFactory.getLogger(AwsS3Storage.class);
	private Props props = Props.getProps();
	private AmazonS3 s3Client;
	private String s3BucketName;
	private static final int MegaBytes = 1024 * 1024;
	private Timer awsS3StorageTimer = new Timer();
	private final ThreadPoolExecutor threadPoolExecutor;
	private final Semaphore semaphore;

	public AwsS3Storage()
	{
		s3BucketName = props.get("s3BucketName", "gltech");
		String accessKey = props.get("accessKey", "AKIAIMCO3L5X25HGADAQ");
		String secretKey = props.get("secretKey", "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");
		s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));

		int activeUploads = props.get("storage.s3.concurrent_uploads", 10);

		TransferQueue<Runnable> queue = new LinkedTransferQueue<>();
		threadPoolExecutor = new TimerThreadPoolExecutor(activeUploads, activeUploads, 1, TimeUnit.MINUTES, queue, new S3UploadThreadFactory(), awsS3StorageTimer);
		semaphore = new Semaphore(activeUploads);
	}

	public void putBucket(ChannelMetaData channelMetaData)
	{
		String key = channelMetaData.getName();

//byte[] jsonData = channelMetaData.toJson().toString().getBytes();
byte[] jsonData = null;
		ByteArrayInputStream bais = new ByteArrayInputStream(jsonData);

		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(jsonData.length);

		s3Client.putObject(s3BucketName, key, bais, metadata);
	}

	public ChannelMetaData getBucket(String channelName)
	{
		String key = channelName;
		S3Object s3Object = null;

		try
		{
			s3Object = s3Client.getObject(s3BucketName, key);
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
//			return new ChannelMetaData(new String(data));
return null;
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public void getPayload(String channelName, String id, OutputStream outputStream)
	{
		String key = keyNameWithUniquePrefix(channelName, id);
		S3Object s3Object = null;

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

		InputStream bis = null;

		try
		{
			bis = new LZFInputStream(s3Object.getObjectContent());

			byte[] buffer = new byte[1024];
			int bytesRead;
			while ((bytesRead = bis.read(buffer)) != -1)
			{
				outputStream.write(buffer, 0, bytesRead);
			}
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
		finally
		{
			IOUtils.closeQuietly(bis);
		}
	}

	public void putPayload(String channelName, String id, InputStream inputStream, Map<String, List<String>> headers)
	{
		String key = keyNameWithUniquePrefix(channelName, id);

		// Set part size to 5 MB.
		int partSize = 5 * MegaBytes;

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
				StreamSplitter.StreamPart streamPart = streamSplitter.next();

				semaphore.acquire();

				try
				{
					Callable<PartETag> uploadPart = new PartUploader(streamPart, key, part, initResponse);
					uploads.add(threadPoolExecutor.submit(uploadPart));
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

			logger.debug("Completed S3 Part Upload key={}, part={}, size={}mb", key, part, streamPart.getSize() / MegaBytes);

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
