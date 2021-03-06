package com.gltech.scale.core.storage.providers;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.gltech.scale.core.cluster.registration.RegistrationService;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.stats.StatsManagerImpl;
import com.gltech.scale.core.model.ModelIO;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.*;

import java.io.FileInputStream;

public class S3Regression
{
	public static void main(String[] args) throws Exception
	{
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.INFO);
		AwsS3Store s3Store = new AwsS3Store(new ModelIO(), new StatsManagerImpl(mock(RegistrationService.class)));

		ChannelMetaData channelMetaData1 = new ChannelMetaData("matt_test", ChannelMetaData.TTL_DAY, false);
		s3Store.putMessages(channelMetaData1, "20130107233405", new FileInputStream(args[0]));

		ChannelMetaData channelMetaData2 = new ChannelMetaData("matt_testb", ChannelMetaData.TTL_DAY, false);
		s3Store.putChannelMetaData(channelMetaData2);
		ChannelMetaData channelMetaData3 = s3Store.getChannelMetaData("matt");

		if (channelMetaData2.equals(channelMetaData3))
		{
			System.out.println("Yay they are equal!");
		}
		else
		{
			System.out.println("I suck!");
		}
	}

}
