package com.gltech.scale.core.cluster;

import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.File;

public class BatchPeriodMapper implements Comparable<BatchPeriodMapper>
{
	private final String channelName;
	private final DateTime nearestPeriodCeiling;

	public BatchPeriodMapper(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling)
	{
		this.channelName = channelMetaData.getName();
		this.nearestPeriodCeiling = nearestPeriodCeiling;
	}

	public BatchPeriodMapper(String nodeName)
	{
		String[] nodeNameParts = nodeName.split("\\|");
		channelName = nodeNameParts[0];
		nearestPeriodCeiling = DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(nodeNameParts[1]);
	}

	static public String nodeNameStripPath(String nodeName)
	{
		return new File(nodeName).getName();
	}

	public String getNodeName()
	{
		StringBuilder cbp = new StringBuilder();
		cbp.append(channelName);
		cbp.append("|");
		cbp.append(nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));

		return cbp.toString();
	}

	public String getChannelName()
	{
		return channelName;
	}

	public DateTime getNearestPeriodCeiling()
	{
		return nearestPeriodCeiling;
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		BatchPeriodMapper that = (BatchPeriodMapper) o;

		if (channelName != null ? !channelName.equals(that.channelName) : that.channelName != null) return false;

		return true;
	}

	public int hashCode()
	{
		return channelName != null ? channelName.hashCode() : 0;
	}

	public int compareTo(BatchPeriodMapper that)
	{
		if (this.equals(that))
		{
			return 0;
		}

		return this.nearestPeriodCeiling.compareTo(that.nearestPeriodCeiling);
	}
}
