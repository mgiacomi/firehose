package com.gltech.scale.monitoring.server;

import javax.servlet.http.HttpServletRequest;

import com.gltech.scale.core.stats.results.ResultsIO;
import com.gltech.scale.core.stats.results.ServerStats;
import com.gltech.scale.monitoring.services.ClusterStatsCallBack;
import com.gltech.scale.monitoring.services.ClusterStatsService;
import com.gltech.scale.monitoring.services.ClusterStatsServiceImpl;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketHandler;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class MonitorWebSocketHandler extends WebSocketHandler implements ClusterStatsCallBack
{
	private final Set<ChatWebSocket> webSockets = new CopyOnWriteArraySet<>();
	private ClusterStatsService clusterStatsService;
	private ResultsIO resultsIO;

	public MonitorWebSocketHandler(Injector injector)
	{
		super();
		this.clusterStatsService = injector.getInstance(ClusterStatsService.class);
		this.resultsIO = injector.getInstance(ResultsIO.class);
	}

	public void registerForServerStatUpdates()
	{
		clusterStatsService.registerCallback(this);
	}

	@Override
	public WebSocket doWebSocketConnect(HttpServletRequest request,
										String protocol)
	{
		return new ChatWebSocket();
	}

	@Override
	public void serverStatsUpdate(ServerStats serverStats)
	{
		for (ChatWebSocket webSocket : webSockets)
		{
			// send a message to the current client WebSocket.
			try
			{
				webSocket.connection.sendMessage(resultsIO.toJson(serverStats));
			}
			catch (IOException e)
			{
				throw Throwables.propagate(e);
			}
		}
	}

	private class ChatWebSocket implements WebSocket.OnTextMessage
	{
		private Connection connection;

		@Override
		public void onOpen(Connection connection)
		{
			// Client (Browser) WebSockets has opened a connection.
			// 1) Store the opened connection
			this.connection = connection;
			// 2) Add ChatWebSocket in the global list of ChatWebSocket
			// instances
			// instance.
			webSockets.add(this);
		}

		@Override
		public void onMessage(String data)
		{
			// Loop for each instance of ChatWebSocket to send message server to
			// each client WebSockets.
			try
			{
				for (ChatWebSocket webSocket : webSockets)
				{
					// send a message to the current client WebSocket.
					webSocket.connection.sendMessage(data);
				}
			}
			catch (IOException x)
			{
				// Error was detected, close the ChatWebSocket client side
				this.connection.close();
			}
		}

		@Override
		public void onClose(int closeCode, String message)
		{
			// Remove ChatWebSocket in the global list of ChatWebSocket
			// instance.
			webSockets.remove(this);
		}
	}
}
