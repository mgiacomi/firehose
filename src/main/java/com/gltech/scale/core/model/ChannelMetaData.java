package com.gltech.scale.core.model;

public class ChannelMetaData
{
	private final String name;
	private final int daysToLive;
	private final boolean redundant;

	public ChannelMetaData(String name, int daysToLive, boolean redundant)
	{
		this.name = name;
		this.daysToLive = daysToLive;
		this.redundant = redundant;
	}

	public String getName()
	{
		return name;
	}

	public int getDaysToLive()
	{
		return daysToLive;
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
}
