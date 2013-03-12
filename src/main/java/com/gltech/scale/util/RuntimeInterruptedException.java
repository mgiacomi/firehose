package com.gltech.scale.util;

public class RuntimeInterruptedException extends RuntimeException
{
	public RuntimeInterruptedException(InterruptedException cause)
	{
		super(cause);
		Thread.currentThread().interrupt();
	}
}
