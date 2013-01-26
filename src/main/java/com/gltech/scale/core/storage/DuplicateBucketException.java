package com.gltech.scale.core.storage;

public class DuplicateBucketException extends StorageException
{
	public DuplicateBucketException(String s)
	{
		super(s);
	}

	public DuplicateBucketException(String s, Throwable throwable)
	{
		super(s, throwable);
	}
}
