package com.gltech.scale.load;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.providers.AwsS3Store;
import com.gltech.scale.util.ModelIO;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;

public class S3Regression
{
	public static void main(String[] args) throws Exception
	{
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.INFO);
		AwsS3Store s3Store = new AwsS3Store(new ModelIO());
		s3Store.putMessages("matt_test", "20130107233405", new FileInputStream(args[0]), new HashMap<String, List<String>>());

		ChannelMetaData channelMetaData = new ChannelMetaData("matt_testb", ChannelMetaData.TTL_DAY, false);

		s3Store.put(channelMetaData);
		ChannelMetaData channelMetaData1 = s3Store.get("matt");

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
