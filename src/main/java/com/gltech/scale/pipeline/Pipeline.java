package com.gltech.scale.pipeline;

/**
 *
 */
public interface Pipeline<T> extends Processor<T>
{

	String getName();

	long getCount();

	double getAverageTimeMillis();
}