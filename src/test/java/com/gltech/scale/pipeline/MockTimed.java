package com.gltech.scale.pipeline;

public class MockTimed implements Timed
{
	private long start;

	public void start()
	{
		start = System.nanoTime();
	}

	public long stop()
	{
		return System.nanoTime() - start;
	}
}
