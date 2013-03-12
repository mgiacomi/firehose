package com.gltech.scale.core.cluster;

import com.gltech.scale.core.storage.StorageException;

public class ClusterException extends StorageException
{
	public ClusterException(String s)
	{
		super(s);
	}

	public ClusterException(String s, Throwable throwable)
	{
		super(s, throwable);
	}
}
