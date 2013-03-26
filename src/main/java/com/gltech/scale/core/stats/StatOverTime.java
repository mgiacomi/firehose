package com.gltech.scale.core.stats;

public interface StatOverTime
{
	void add(long total);

	String getStatName();

	void cleanOldThanTwoHours();
}
