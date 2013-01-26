package com.gltech.scale.core.util;

public class RuntimeInterruptedException extends RuntimeException
{
	public RuntimeInterruptedException(InterruptedException cause)
	{
		super(cause);
		Thread.currentThread().interrupt();
	}
}
