package com.gltech.scale.core.rope;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.gltech.scale.core.coordination.registration.ServiceMetaData;

public class PrimaryBackupSet
{
	private final ServiceMetaData primary;
	private final ServiceMetaData backup;

	@JsonCreator
	public PrimaryBackupSet(@JsonProperty("primary") ServiceMetaData primary, @JsonProperty("backup") ServiceMetaData backup)
	{
		this.primary = primary;
		this.backup = backup;
	}

	public ServiceMetaData getPrimary()
	{
		return primary;
	}

	public ServiceMetaData getBackup()
	{
		return backup;
	}
}