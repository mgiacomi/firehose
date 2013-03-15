package com.gltech.scale.load;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.gltech.scale.core.model.ChannelMetaData;
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

//		ChannelMetaData channelMetaData = new ChannelMetaData(json);
ChannelMetaData channelMetaData = null;


		s3Storage.putBucket(channelMetaData);
		ChannelMetaData channelMetaData1 = s3Storage.getBucket("matt");

		if (channelMetaData.equals(channelMetaData1))
		{
			System.out.println("Yay they are equal!");
		}
		else
		{
			System.out.println("I suck!");
		}
	}

}
