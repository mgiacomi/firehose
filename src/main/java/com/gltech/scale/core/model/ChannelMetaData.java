package com.gltech.scale.core.model;

import com.dyuproject.protostuff.Tag;

public class ChannelMetaData
{
	@Tag(1)
	private final String name;
	@Tag(2)
	private final int daysToLive;
	@Tag(3)
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

	public String toString()
	{
		return "ChannelMetaData{" +
				"name='" + name + '\'' +
				", daysToLive=" + daysToLive +
				", redundant=" + redundant +
				'}';
	}
}
