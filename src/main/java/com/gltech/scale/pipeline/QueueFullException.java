package com.gltech.scale.pipeline;

public class QueueFullException extends Exception
{

	public QueueFullException(String message)
	{
		super(message);
	}

	public QueueFullException(String message, Throwable cause)
	{
		super(message, cause);
	}
}
