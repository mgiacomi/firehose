package com.gltech.scale.monitoring;

public class AvgCountStat
{
	private final long total;
	private final long count;

	public AvgCountStat(long total, long count)
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
