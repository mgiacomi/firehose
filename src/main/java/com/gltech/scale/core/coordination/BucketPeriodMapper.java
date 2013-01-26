package com.gltech.scale.core.coordination;

import com.gltech.scale.core.storage.BucketMetaData;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.File;

public class BucketPeriodMapper implements Comparable<BucketPeriodMapper>
{
	private final String customer;
	private final String bucket;
	private final DateTime nearestPeriodCeiling;

	public BucketPeriodMapper(BucketMetaData bucketMetaData, DateTime nearestPeriodCeiling)
	{
		this.customer = bucketMetaData.getCustomer();
		this.bucket = bucketMetaData.getBucket();
		this.nearestPeriodCeiling = nearestPeriodCeiling;
	}

	public BucketPeriodMapper(String nodeName)
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

	public int compareTo(BucketPeriodMapper that)
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

		BucketPeriodMapper that = (BucketPeriodMapper) o;

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
