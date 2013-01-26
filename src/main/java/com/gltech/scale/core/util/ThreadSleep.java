package com.gltech.scale.core.util;

public class ThreadSleep
{
	public static void sleep(int millis)
	{
		try
		{
			Thread.sleep(millis);
		}
		catch (InterruptedException e)
		{
			throw new RuntimeInterruptedException(e);
		}
	}
}
