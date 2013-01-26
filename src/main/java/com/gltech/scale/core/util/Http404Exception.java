package com.gltech.scale.core.util;

public class Http404Exception extends RuntimeException
{
	public Http404Exception(String s)
	{
		super(s);
	}

	public Http404Exception(String s, Throwable throwable)
	{
		super(s, throwable);
	}
}
