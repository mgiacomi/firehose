package com.gltech.scale.core.storage.bytearray;

import com.gltech.scale.core.storage.StorageException;

public class InvalidVersionException extends StorageException
{
	public InvalidVersionException(String s)
	{
		super(s);
	}

	public InvalidVersionException(String s, Throwable throwable)
	{
		super(s, throwable);
	}
}
