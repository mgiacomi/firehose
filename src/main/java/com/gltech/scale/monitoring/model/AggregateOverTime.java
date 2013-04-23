package com.gltech.scale.monitoring.model;

import com.dyuproject.protostuff.Tag;
import com.gltech.scale.core.stats.results.AvgStat;
import com.gltech.scale.core.stats.results.OverTime;

public class AggregateOverTime
{
	@Tag(1)
	private String name;
	@Tag(2)
	private String unitOfMeasure;
	@Tag(3)
	AvgStat avgSec5;
	@Tag(4)
	AvgStat avgMin1;
	@Tag(5)
	AvgStat avgMin5;
	@Tag(6)
	AvgStat avgMin30;
	@Tag(7)
	AvgStat avgHour2;
	@Tag(8)
	long totalSec5;
	@Tag(9)
	long totalMin1;
	@Tag(10)
	long totalMin5;
	@Tag(11)
	long totalMin30;
	@Tag(12)
	long totalHour2;
	@Tag(13)
	long highSec5;
	@Tag(14)
	long highMin1;
	@Tag(15)
	long highMin5;
	@Tag(16)
	long highMin30;
	@Tag(17)
	long highHour2;
	@Tag(18)
	long lowSec5;
	@Tag(19)
	long lowMin1;
	@Tag(20)
	long lowMin5;
	@Tag(21)
	long lowMin30;
	@Tag(22)
	long lowHour2;

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public String getUnitOfMeasure()
	{
		return unitOfMeasure;
	}

	public void setUnitOfMeasure(String unitOfMeasure)
	{
		this.unitOfMeasure = unitOfMeasure;
	}

	public AvgStat getAvgSec5()
	{
		return avgSec5;
	}

	public void setAvgSec5(AvgStat avgSec5)
	{
		this.avgSec5 = avgSec5;
	}

	public AvgStat getAvgMin1()
	{
		return avgMin1;
	}

	public void setAvgMin1(AvgStat avgMin1)
	{
		this.avgMin1 = avgMin1;
	}

	public AvgStat getAvgMin5()
	{
		return avgMin5;
	}

	public void setAvgMin5(AvgStat avgMin5)
	{
		this.avgMin5 = avgMin5;
	}

	public AvgStat getAvgMin30()
	{
		return avgMin30;
	}

	public void setAvgMin30(AvgStat avgMin30)
	{
		this.avgMin30 = avgMin30;
	}

	public AvgStat getAvgHour2()
	{
		return avgHour2;
	}

	public void setAvgHour2(AvgStat avgHour2)
	{
		this.avgHour2 = avgHour2;
	}

	public long getTotalSec5()
	{
		return totalSec5;
	}

	public void setTotalSec5(long totalSec5)
	{
		this.totalSec5 = totalSec5;
	}

	public long getTotalMin1()
	{
		return totalMin1;
	}

	public void setTotalMin1(long totalMin1)
	{
		this.totalMin1 = totalMin1;
	}

	public long getTotalMin5()
	{
		return totalMin5;
	}

	public void setTotalMin5(long totalMin5)
	{
		this.totalMin5 = totalMin5;
	}

	public long getTotalMin30()
	{
		return totalMin30;
	}

	public void setTotalMin30(long totalMin30)
	{
		this.totalMin30 = totalMin30;
	}

	public long getTotalHour2()
	{
		return totalHour2;
	}

	public void setTotalHour2(long totalHour2)
	{
		this.totalHour2 = totalHour2;
	}

	public long getHighSec5()
	{
		return highSec5;
	}

	public void setHighSec5(long highSec5)
	{
		this.highSec5 = highSec5;
	}

	public long getHighMin1()
	{
		return highMin1;
	}

	public void setHighMin1(long highMin1)
	{
		this.highMin1 = highMin1;
	}

	public long getHighMin5()
	{
		return highMin5;
	}

	public void setHighMin5(long highMin5)
	{
		this.highMin5 = highMin5;
	}

	public long getHighMin30()
	{
		return highMin30;
	}

	public void setHighMin30(long highMin30)
	{
		this.highMin30 = highMin30;
	}

	public long getHighHour2()
	{
		return highHour2;
	}

	public void setHighHour2(long highHour2)
	{
		this.highHour2 = highHour2;
	}

	public long getLowSec5()
	{
		return lowSec5;
	}

	public void setLowSec5(long lowSec5)
	{
		this.lowSec5 = lowSec5;
	}

	public long getLowMin1()
	{
		return lowMin1;
	}

	public void setLowMin1(long lowMin1)
	{
		this.lowMin1 = lowMin1;
	}

	public long getLowMin5()
	{
		return lowMin5;
	}

	public void setLowMin5(long lowMin5)
	{
		this.lowMin5 = lowMin5;
	}

	public long getLowMin30()
	{
		return lowMin30;
	}

	public void setLowMin30(long lowMin30)
	{
		this.lowMin30 = lowMin30;
	}

	public long getLowHour2()
	{
		return lowHour2;
	}

	public void setLowHour2(long lowHour2)
	{
		this.lowHour2 = lowHour2;
	}
}
