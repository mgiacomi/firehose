package com.gltech.scale.monitoring.results;

public class AvgStat
{
	private long total;
	private long count;

	public AvgStat(long total, long count)
	{
		this.total = total;
		this.count = count;
	}

	public long getTotal()
	{
		return total;
	}

	public long getAverage()
	{
		if (total == 0 || count == 0)
		{
			return 0;
		}

		return total / count;
	}

	public long getCount()
	{
		return count;
	}
}
