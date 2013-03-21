package com.gltech.scale.core.storage;

public class KeyAlreadyExistsException extends StorageException
{
	public KeyAlreadyExistsException(String s)
	{
		super(s);
	}

	public KeyAlreadyExistsException(String s, Throwable throwable)
	{
		super(s, throwable);
	}
}
