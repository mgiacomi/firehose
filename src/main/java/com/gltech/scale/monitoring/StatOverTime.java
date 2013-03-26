package com.gltech.scale.monitoring;

public interface StatOverTime
{
	void add(long total);

	String getStatName();

	void cleanOldThanTwoHours();
}
