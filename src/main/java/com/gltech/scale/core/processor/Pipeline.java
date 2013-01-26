package com.gltech.scale.core.processor;

/**
 *
 */
public interface Pipeline<T> extends Processor<T>
{

	String getName();

	long getCount();

	double getAverageTimeMillis();
}