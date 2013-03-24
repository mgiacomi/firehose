package com.gltech.scale.monitoring.results;

public class OverTime<T>
{
	private String statName;
	private T min1;
	private T min5;
	private T min30;
	private T hour2;

	public OverTime(String statName, T min1, T min5, T min30, T hour2)
	{
		this.statName = statName;
		this.min1 = min1;
		this.min5 = min5;
		this.min30 = min30;
		this.hour2 = hour2;
	}

	public String getStatName()
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
