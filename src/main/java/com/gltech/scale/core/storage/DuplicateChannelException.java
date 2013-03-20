package com.gltech.scale.core.storage;

public class DuplicateChannelException extends StorageException
{
	public DuplicateChannelException(String s)
	{
		super(s);
	}

	public DuplicateChannelException(String s, Throwable throwable)
	{
		super(s, throwable);
	}
}
