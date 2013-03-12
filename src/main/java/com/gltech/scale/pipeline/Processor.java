package com.gltech.scale.pipeline;

/**
 * A Processor does work.  End users should implement the actual work of the system in a Processor.
 */
public interface Processor<T>
{
	void process(T t) throws Exception;
}
