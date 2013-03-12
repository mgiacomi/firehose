package com.gltech.scale.pipeline;

import com.gltech.scale.lifecycle.LifeCycleManager;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class PipelineBuilderIntegrationTest
{

	@Test(expected = IllegalStateException.class)
	public void testNullDelegate() throws Exception
	{
		new PipelineBuilder(null, "").build();
	}

	@Test(expected = IllegalStateException.class)
	public void testNullName() throws Exception
	{
		new PipelineBuilder(new CountingProcessor(), null).build();
	}

	@Test(expected = IllegalStateException.class)
	public void testAsynchInvalidThread() throws Exception
	{
		new PipelineBuilder(new CountingProcessor(), "").asynchronous(0, 1).build();
	}

	@Test(expected = IllegalStateException.class)
	public void testAsynchInvalidQueue() throws Exception
	{
		new PipelineBuilder(new CountingProcessor(), "").asynchronous(1, 0).build();
	}

	@Test
	public void testAsynchStatistics() throws Exception
	{
		CountingProcessor<MockTimed> countingProcessor = new CountingProcessor<>();
		Pipeline<MockTimed> processor = new PipelineBuilder(countingProcessor, "")
				.statistics("test")
				.asynchronous(1, 1000).build();

		for (int i = 0; i < 1000; i++)
		{
			processor.process(new MockTimed());
		}
		LifeCycleManager.getInstance().shutdown();
		assertEquals(1000, countingProcessor.getCount());
		assertEquals(1000, processor.getCount());
		assertEquals(5, processor.getAverageTimeMillis(), 4.9);
	}


}
