package com.gltech.scale.core.coordination;

import com.gltech.scale.core.util.Props;
import org.joda.time.DateTime;

public class TimePeriodUtils
{
	private Props props = Props.getProps();

	static public DateTime nearestPeriodCeiling(DateTime dateTime, int periodSeconds)
	{
		dateTime = dateTime.withMillisOfSecond(0);
		double sec = dateTime.getSecondOfMinute();
		int newSec = ((int) Math.ceil(sec / periodSeconds)) * periodSeconds;

		if (newSec == 60)
		{
			dateTime = dateTime.plusMinutes(1);
			newSec = 0;
		}


		return dateTime.withSecondOfMinute(newSec);
	}

	public DateTime nearestPeriodCeiling(DateTime dateTime)
	{
		int periodSeconds = props.get("coordination.period_seconds", 5);
		return nearestPeriodCeiling(dateTime, periodSeconds);
	}


}
