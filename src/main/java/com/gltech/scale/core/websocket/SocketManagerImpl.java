package com.gltech.scale.core.websocket;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.lifecycle.LifeCycle;
import com.gltech.scale.util.Props;
import com.google.common.base.Throwables;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SocketManagerImpl implements SocketManager, SocketState, LifeCycle
{
	private static final Logger logger = LoggerFactory.getLogger(SocketManagerImpl.class);
	private ConcurrentMap<UUID, AggregatorSocketData> aggregatorMap = new ConcurrentHashMap<>();
	private static final Object socketCreationLock = new Object();
	private Props props = Props.getProps();

	@Override
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
						String destUri = "ws://" + serviceMetaData.getListenAddress() + ":" + serviceMetaData.getListenPort() + "/socket/aggregator";
						WebSocketClient client = new WebSocketClient();
						AggregatorSocket socket = new AggregatorSocket(serviceMetaData.getWorkerId(), this);

						try
						{
							client.start();
							URI echoUri = new URI(destUri);
							ClientUpgradeRequest request = new ClientUpgradeRequest();
							client.connect(socket, echoUri, request);

							AggregatorSocketData aggregatorSocketData = new AggregatorSocketData(socket, client);
							aggregatorMap.put(serviceMetaData.getWorkerId(), aggregatorSocketData);

							double elapseSeconds = 0;
							int waitResponseSeconds = props.get("inbound.socket_wait_response_secs", Defaults.INBOUND_SOCKET_WAIT_RESPONSE_SECS);
							long timer = System.nanoTime();

							while (elapseSeconds < waitResponseSeconds)
							{
								elapseSeconds = (double) ((System.nanoTime() - timer)) / 1000000000.0;

								try
								{
									if (aggregatorSocketData.session != null && aggregatorSocketData.session.isOpen())
									{
										return aggregatorSocketData.aggregatorSocket;
									}
								}
								catch (NullPointerException e)
								{
									logger.error("Failed to get AggregatorSocketData in the map.  Must gotten an error before it was opened.");
								}

								Thread.sleep(10);
							}

							throw new RuntimeException("Failed to connect to aggregator " + destUri);
						}
						catch (Exception e)
						{
							throw Throwables.propagate(e);
						}
					}
				}
			}

			try
			{
				if (aggregatorMap.get(serviceMetaData.getWorkerId()).session != null && aggregatorMap.get(serviceMetaData.getWorkerId()).session.isOpen())
				{
					return aggregatorMap.get(serviceMetaData.getWorkerId()).aggregatorSocket;
				}
			}
			catch (NullPointerException e)
			{
				// NPE means it was remove between the time of the if and get.  Just try again.
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
		try
		{
			aggregatorMap.get(workerId).session = session;
		}
		catch (Exception e)
		{
			logger.error("Failed to add WebSocket session to AggregatorSocketData.", e);
		}
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
		private Session session;

		private AggregatorSocketData(AggregatorSocket aggregatorSocket, WebSocketClient webSocketClient)
		{
			this.aggregatorSocket = aggregatorSocket;
			this.webSocketClient = webSocketClient;
		}
	}
}
