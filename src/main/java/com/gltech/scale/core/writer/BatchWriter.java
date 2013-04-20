package com.gltech.scale.core.writer;

import com.gltech.scale.core.stats.AvgStatOverTime;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.stats.CountStatOverTime;
import org.joda.time.DateTime;

import java.util.concurrent.Callable;

public interface BatchWriter extends Callable<Object>
{
	void assign(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling);

	void setChannelStat(AvgStatOverTime channelStat);

	void setMessagesWrittenStat(CountStatOverTime messagesWrittenStat);

	void setBytesWrittenStat(CountStatOverTime bytesWrittenStat);
}
