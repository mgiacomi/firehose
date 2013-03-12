package com.gltech.scale.ganglia;

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
