package com.gltech.scale.util;

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
