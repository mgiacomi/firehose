package com.gltech.scale.core.coordination;

import com.gltech.scale.core.storage.StorageException;

public class CoordinationException extends StorageException
{
	public CoordinationException(String s)
	{
		super(s);
	}

	public CoordinationException(String s, Throwable throwable)
	{
		super(s, throwable);
	}
}
