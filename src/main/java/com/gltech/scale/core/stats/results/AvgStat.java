package com.gltech.scale.core.stats.results;

import com.dyuproject.protostuff.Tag;

public class AvgStat
{
	@Tag(1)
	private long total;
	@Tag(2)
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
