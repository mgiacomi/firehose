package com.gltech.scale.core.monitor;

import ganglia.gmetric.GMetricSlope;
import ganglia.gmetric.GMetricType;

/**
 *
 */
public class PublishMetric
{
	private String name;
	String units;
	String groupName;
	PublishCallback valueCallback;

	GMetricType type = GMetricType.DOUBLE;
	GMetricSlope slope = GMetricSlope.BOTH;

	public PublishMetric(String name, String groupName, String units, PublishCallback valueCallback)
	{
		this.name = name;
		this.groupName = groupName;
		this.units = units;
		this.valueCallback = valueCallback;
	}

	public GMetricType getType()
	{
		return type;
	}

	public void setType(GMetricType type)
	{
		this.type = type;
	}

	public GMetricSlope getSlope()
	{
		return slope;
	}

	public void setSlope(GMetricSlope slope)
	{
		this.slope = slope;
	}

	public String getGroupName()
	{
		return groupName;
	}

	public String getName()
	{
		return name;
	}

	public String getUnits()
	{
		return units;
	}

	public PublishCallback getValueCallback()
	{
		return valueCallback;
	}

	public String toString()
	{
		return "PublishMetric{" +
				"groupName='" + groupName + '\'' +
				", name='" + name + '\'' +
				", units='" + units + '\'' +
				", valueCallback=" + valueCallback +
				", type=" + type +
				", slope=" + slope +
				'}';
	}

	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		PublishMetric that = (PublishMetric) o;

		if (!groupName.equals(that.groupName))
			return false;
		if (!name.equals(that.name))
			return false;

		return true;
	}

	public int hashCode()
	{
		int result = name.hashCode();
		result = 31 * result + groupName.hashCode();
		return result;
	}
}
