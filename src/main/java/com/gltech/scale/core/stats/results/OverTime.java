package com.gltech.scale.core.stats.results;

import com.dyuproject.protostuff.Tag;

public class OverTime<T>
{
	@Tag(1)
	private String statName;
	@Tag(2)
	private T min1;
	@Tag(3)
	private T min5;
	@Tag(4)
	private T min30;
	@Tag(5)
	private T hour2;

	public OverTime(String statName, T min1, T min5, T min30, T hour2)
	{
		this.statName = statName;
		this.min1 = min1;
		this.min5 = min5;
		this.min30 = min30;
		this.hour2 = hour2;
	}

	public String getName()
	{
		return statName;
	}

	public T getMin1()
	{
		return min1;
	}

	public T getMin5()
	{
		return min5;
	}

	public T getMin30()
	{
		return min30;
	}

	public T getHour2()
	{
		return hour2;
	}
}
