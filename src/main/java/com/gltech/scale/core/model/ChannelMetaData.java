package com.gltech.scale.core.model;

import com.dyuproject.protostuff.Tag;

public class ChannelMetaData
{
	static public final String TTL_DAY = "day";
	static public final String TTL_WEEK = "week";
	static public final String TTL_MONTH = "month";
	static public final String TTL_YEAR = "year";
	static public final String TTL_FOREVER = "forever";

	@Tag(1)
	private final String name;
	@Tag(2)
	private final String ttl;
	@Tag(3)
	private final boolean redundant;

	public ChannelMetaData()
	{
		this.name = null;
		this.ttl = null;
		this.redundant = false;
	}

	public ChannelMetaData(String name, String ttl, boolean redundant)
	{
		this.name = name;
		this.ttl = validateTTL(ttl);
		this.redundant = redundant;
	}

	public String getName()
	{
		return name;
	}

	public String getTtl()
	{
		return ttl;
	}

	public boolean isRedundant()
	{
		return redundant;
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ChannelMetaData that = (ChannelMetaData) o;

		if (name != null ? !name.equals(that.name) : that.name != null) return false;

		return true;
	}

	public int hashCode()
	{
		return name != null ? name.hashCode() : 0;
	}

	public String toString()
	{
		return "ChannelMetaData{" +
				"name='" + name + '\'' +
				", ttl=" + ttl +
				", redundant=" + redundant +
				'}';
	}

	private String validateTTL(String ttl)
	{
		if (TTL_DAY.equals(ttl) || TTL_MONTH.equals(ttl) || TTL_WEEK.equals(ttl) || TTL_YEAR.equals(ttl) || TTL_FOREVER.equals(ttl))
		{
			return ttl;
		}

		throw new IllegalArgumentException("TTL is not supported: " + ttl);
	}
}
