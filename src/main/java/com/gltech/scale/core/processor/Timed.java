package com.gltech.scale.core.processor;

/**
 *
 */
public interface Timed
{
	/**
	 * start is called at the top of the Processor stack.
	 */
	void start();

	/**
	 * stop is called after Processing, returns the time in nanos.
	 *
	 * @return
	 */
	long stop();
}
