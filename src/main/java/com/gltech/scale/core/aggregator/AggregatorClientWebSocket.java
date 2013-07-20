package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.ModelIO;
import com.gltech.scale.core.websocket.ResponseCallback;
import com.gltech.scale.core.websocket.SocketManager;
import com.gltech.scale.util.Props;
import com.google.inject.Inject;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class AggregatorClientWebSocket implements AggregatorClient
{
	private static final Logger logger = LoggerFactory.getLogger(AggregatorClientWebSocket.class);
	private AtomicInteger atomicInteger = new AtomicInteger();
	private SocketManager socketManager;
	private ModelIO modelIO;
	private Props props = Props.getProps();

	@Inject
	public AggregatorClientWebSocket(SocketManager socketManager, ModelIO modelIO)
	{
		this.socketManager = socketManager;
		this.modelIO = modelIO;
	}

	@Override
	public void sendMessage(ServiceMetaData primary, String channelName, DateTime nearestPeriodCeiling, Message message)
	{
		sendMessage(primary, null, channelName, nearestPeriodCeiling, message);
	}

	@Override
	public void sendMessage(ServiceMetaData primary, ServiceMetaData backup, String channelName, DateTime nearestPeriodCeiling, Message message)
	{
		if (primary == null)
		{
			throw new RuntimeException("Primary Aggregator can not be null.");
		}

		int id = atomicInteger.getAndIncrement();
		byte[] messageBytes = modelIO.toBytes(message);
		ResponseCallback callback = new ResponseCallback(primary, backup);

		com.gltech.scale.core.websocket.AggregatorSocket primarySocket = socketManager.getAggregatorSocket(primary);
		primarySocket.sendPrimary(id, channelName, nearestPeriodCeiling, messageBytes, callback);

		if (backup != null)
		{
			com.gltech.scale.core.websocket.AggregatorSocket backupSocket = socketManager.getAggregatorSocket(backup);
			backupSocket.sendBackup(id, channelName, nearestPeriodCeiling, messageBytes, callback);
		}

		double elapseSeconds = 0;
		int waitResponseSeconds = props.get("inbound.socket_wait_response_secs", Defaults.INBOUND_SOCKET_WAIT_RESPONSE_SECS);
		long timer = System.nanoTime();

		while (elapseSeconds < waitResponseSeconds)
		{
			elapseSeconds = (double) ((System.nanoTime() - timer)) / 1000000000.0;

			if (callback.hasResponse() && callback.gotAck())
			{
				// We have an ACK all is good. Move on.
				return;
			}

			//Thread.yield();
			try
			{
				Thread.sleep(100);
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}

		callback.logErrors();

		throw new RuntimeException("No success ACK's were returned within " + elapseSeconds + " seconds from primary or backup aggregator.  primary={" + primary + "} backup={" + backup + "}");
	}
}
