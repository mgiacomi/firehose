package com.gltech.scale.core.load;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.gltech.scale.core.storage.BucketMetaData;
import com.gltech.scale.core.storage.stream.AwsS3Storage;
import org.slf4j.LoggerFactory;

public class S3Regression
{
	public static void main(String[] args) throws Exception
	{
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.INFO);
		AwsS3Storage s3Storage = new AwsS3Storage();
//		s3Storage.putPayload("matt", "test", "20130107233405", new FileInputStream(args[0]), new HashMap<String, List<String>>());

		String json = "{\"customer\":\"matt\", \"bucket\":\"testb\", \"bucketType\":\"EvEntSet\", \"redundancy\":\"singlewrite\", \"mediaType\":\"application/json\"}";

		BucketMetaData bucketMetaData = new BucketMetaData(json);

		s3Storage.putBucket(bucketMetaData);
		BucketMetaData bucketMetaData1 = s3Storage.getBucket("matt", "testb");

		if (bucketMetaData.equals(bucketMetaData1))
		{
			System.out.println("Yay they are equal!");
		}
		else
		{
			System.out.println("I suck!");
		}
	}

}