package com.gltech.scale.core.websocket;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ResponseCallback
{
	private static final Logger logger = LoggerFactory.getLogger(ResponseCallback.class);
	private final ServiceMetaData primary;
	private final ServiceMetaData backup;
	private SocketResponse primaryResponse = null;
	private SocketResponse backupResponse = null;
	private Future<Void> primaryRequestFuture = null;
	private Future<Void> backupRequestFuture = null;

	public ResponseCallback(ServiceMetaData primary, ServiceMetaData backup)
	{
		this.primary = primary;
		this.backup = backup;
	}

	public boolean hasResponse()
	{
		return primaryResponse != null || backupResponse != null;
	}

	public boolean gotAck()
	{
		if (primaryResponse != null && primaryResponse.isAck())
		{
			return true;
		}
		if (backupResponse != null && backupResponse.isAck())
		{
			return true;
		}

		return false;
	}

	public boolean logErrors()
	{
		if (primaryResponse != null)
		{
			if (primaryResponse.isError())
			{
				logger.error("Response error from primary aggregator {}.  Error={}", primary, primaryResponse.getData());
			}

			try
			{
				primaryRequestFuture.get(2, TimeUnit.SECONDS);
			}
			catch (Exception e)
			{
				logger.error("Failed to get future for primary aggregator {}", primary, e);
			}
		}

		if (backupResponse != null)
		{
			if (backupResponse.isError())
			{
				logger.error("Response error from backup aggregator {}.  Error={}", backup, backupResponse.getData());
			}

			try
			{
				backupRequestFuture.get(2, TimeUnit.SECONDS);
			}
			catch (Exception e)
			{
				logger.error("Failed to get future for backup aggregator {}", backup, e);
			}
		}

		return false;
	}

	public void setRequestFuture(UUID workerId, Future<Void> future)
	{
		if (workerId.equals(primary.getWorkerId()))
		{
			primaryRequestFuture = future;
		}

		if (backup != null && workerId.equals(backup.getWorkerId()))
		{
			backupRequestFuture = future;
		}

		throw new RuntimeException("Could not find ServiceMetaData for workerId: " + workerId);
	}

	public void setResponse(UUID workerId, SocketResponse response)
	{
		if (workerId.equals(primary.getWorkerId()))
		{
			primaryResponse = response;
		}

		if (backup != null && workerId.equals(backup.getWorkerId()))
		{
			backupResponse = response;
		}

		throw new RuntimeException("Could not find ServiceMetaData for workerId: " + workerId);
	}

	public ServiceMetaData getPrimary()
	{
		return primary;
	}

	public ServiceMetaData getBackup()
	{
		return backup;
	}

	public SocketResponse getPrimaryResponse()
	{
		return primaryResponse;
	}

	public SocketResponse getBackupResponse()
	{
		return backupResponse;
	}

	public Future<Void> getPrimaryRequestFuture()
	{
		return primaryRequestFuture;
	}

	public Future<Void> getBackupRequestFuture()
	{
		return backupRequestFuture;
	}
}
