package com.gltech.scale.monitoring;

public interface StatOverTime
{
	String getStatName();

	void cleanOldThanTwoHours();
}
