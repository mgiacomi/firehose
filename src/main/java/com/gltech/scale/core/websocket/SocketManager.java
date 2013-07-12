package com.gltech.scale.core.websocket;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.lifecycle.LifeCycle;
import com.google.inject.Inject;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SocketManager implements SocketState, LifeCycle
{
	private ConcurrentMap<UUID, AggregatorSocketData> aggregatorMap = new ConcurrentHashMap<>();
	private static final Object socketCreationLock = new Object();
	private SocketIO socketIO;

	@Inject
	public SocketManager(SocketIO socketIO)
	{
		this.socketIO = socketIO;
	}

	public AggregatorSocket getAggregatorSocket(ServiceMetaData serviceMetaData)
	{
		int retries = 3;
		while (retries > 0)
		{
			if (!aggregatorMap.containsKey(serviceMetaData.getWorkerId()))
			{
				synchronized (socketCreationLock)
				{
					if (!aggregatorMap.containsKey(serviceMetaData.getWorkerId()))
					{
						String destUri = "ws://" + serviceMetaData.getListenAddress() + ":" + serviceMetaData.getListenPort();
						WebSocketClient client = new WebSocketClient();
						AggregatorSocket socket = new AggregatorSocket(serviceMetaData.getWorkerId(), this, socketIO);

						try
						{
							client.start();
							URI echoUri = new URI(destUri);
							ClientUpgradeRequest request = new ClientUpgradeRequest();
							client.connect(socket, echoUri, request);

							AggregatorSocketData aggregatorSocketData = new AggregatorSocketData(socket, client);
							aggregatorMap.put(serviceMetaData.getWorkerId(), aggregatorSocketData);
						}
						catch (Throwable t)
						{
							t.printStackTrace();
						}
					}
				}
			}

			try
			{
				return aggregatorMap.get(serviceMetaData.getWorkerId()).aggregatorSocket;
			}
			catch (NullPointerException e)
			{
				// The connection must have errored out before we could return it.
				// Let's loop again
			}
			retries--;
		}

		throw new RuntimeException("Not able to establish a WebSocket connection after three retries.");
	}

	@Override
	public void shutdown()
	{
		for (UUID uuid : aggregatorMap.keySet())
		{
			WebSocketClient client = aggregatorMap.get(uuid).webSocketClient;

			try
			{
				client.stop();
			}
			catch (Exception e)
			{
				// We couldn't stop the client.  Big deal...
			}

			aggregatorMap.remove(uuid);
		}
	}


	@Override
	public void onConnect(UUID workerId, Session session)
	{
		// To be implemented
	}

	@Override
	public void onClose(UUID workerId, Session session, int statusCode, String reason)
	{
		aggregatorMap.remove(workerId);
	}

	@Override
	public void onError(UUID workerId, Session session, Throwable error)
	{
		aggregatorMap.remove(workerId);
	}

	private class AggregatorSocketData
	{
		private AggregatorSocket aggregatorSocket;
		private WebSocketClient webSocketClient;

		private AggregatorSocketData(AggregatorSocket aggregatorSocket, WebSocketClient webSocketClient)
		{
			this.aggregatorSocket = aggregatorSocket;
			this.webSocketClient = webSocketClient;
		}
	}
}
