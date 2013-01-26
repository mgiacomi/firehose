package com.gltech.scale.core.monitor;

/**
 *
 */
public interface PublishCallback
{

	/**
	 * This gets called every time we want to push a value out.
	 *
	 * @return The value as a string, whether it is an integer, double, etc
	 */
	String getValue();
}
