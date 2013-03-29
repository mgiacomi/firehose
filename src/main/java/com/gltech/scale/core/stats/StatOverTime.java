package com.gltech.scale.core.stats;

public interface StatOverTime
{
	void add(long total);

	void startTimer();

	void stopTimer();

	String getName();

	String getUnitOfMeasure();

	void cleanOldThanTwoHours();
}
