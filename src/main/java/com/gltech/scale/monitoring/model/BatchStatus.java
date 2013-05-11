package com.gltech.scale.monitoring.model;

import com.dyuproject.protostuff.Tag;
import com.gltech.scale.core.model.BatchMetaData;

public class BatchStatus
{
	@Tag(1)
	private BatchMetaData batchMetaData;
	@Tag(2)
	private boolean registered;
	@Tag(3)
	private boolean primary;
	@Tag(4)
	private String hostname;
	@Tag(5)
	private String workerId;

	public BatchMetaData getBatchMetaData()
	{
		return batchMetaData;
	}

	public void setBatchMetaData(BatchMetaData batchMetaData)
	{
		this.batchMetaData = batchMetaData;
	}

	public boolean isRegistered()
	{
		return registered;
	}

	public void setRegistered(boolean registered)
	{
		this.registered = registered;
	}

	public boolean isPrimary()
	{
		return primary;
	}

	public void setPrimary(boolean primary)
	{
		this.primary = primary;
	}

	public String getHostname()
	{
		return hostname;
	}

	public void setHostname(String hostname)
	{
		this.hostname = hostname;
	}

	public String getWorkerId()
	{
		return workerId;
	}

	public void setWorkerId(String workerId)
	{
		this.workerId = workerId;
	}
}
