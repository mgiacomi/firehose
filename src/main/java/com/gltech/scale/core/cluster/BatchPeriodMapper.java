package com.gltech.scale.core.cluster;

import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.File;

public class BatchPeriodMapper implements Comparable<BatchPeriodMapper>
{
	private final String customer;
	private final String bucket;
	private final DateTime nearestPeriodCeiling;

	public BatchPeriodMapper(ChannelMetaData channelMetaData, DateTime nearestPeriodCeiling)
	{
		this.customer = channelMetaData.getCustomer();
		this.bucket = channelMetaData.getBucket();
		this.nearestPeriodCeiling = nearestPeriodCeiling;
	}

	public BatchPeriodMapper(String nodeName)
	{
		String[] nodeNameParts = nodeName.split("\\|");
		customer = nodeNameParts[0];
		bucket = nodeNameParts[1];
		nearestPeriodCeiling = DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(nodeNameParts[2]);
	}

	static public String nodeNameStripPath(String nodeName)
	{
		return new File(nodeName).getName();
	}

	public String getNodeName()
	{
		StringBuilder cbp = new StringBuilder();
		cbp.append(customer);
		cbp.append("|");
		cbp.append(bucket);
		cbp.append("|");
		cbp.append(nearestPeriodCeiling.toString(DateTimeFormat.forPattern("yyyyMMddHHmmss")));

		return cbp.toString();
	}

	public int compareTo(BatchPeriodMapper that)
	{
		if (this.equals(that))
		{
			return 0;
		}

		return this.nearestPeriodCeiling.compareTo(that.nearestPeriodCeiling);
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		BatchPeriodMapper that = (BatchPeriodMapper) o;

		if (bucket != null ? !bucket.equals(that.bucket) : that.bucket != null) return false;
		if (customer != null ? !customer.equals(that.customer) : that.customer != null) return false;
		if (nearestPeriodCeiling != null ? !nearestPeriodCeiling.equals(that.nearestPeriodCeiling) : that.nearestPeriodCeiling != null)
			return false;

		return true;
	}

	public int hashCode()
	{
		int result = customer != null ? customer.hashCode() : 0;
		result = 31 * result + (bucket != null ? bucket.hashCode() : 0);
		result = 31 * result + (nearestPeriodCeiling != null ? nearestPeriodCeiling.hashCode() : 0);
		return result;
	}

	public String getCustomer()
	{
		return customer;
	}

	public String getBucket()
	{
		return bucket;
	}

	public DateTime getNearestPeriodCeiling()
	{
		return nearestPeriodCeiling;
	}
}
