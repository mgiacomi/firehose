package com.gltech.scale.monitoring.server;

import com.google.inject.Injector;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.eclipse.jetty.websocket.api.UpgradeResponse;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

public class StatsPushHandler extends WebSocketHandler
{
	private Injector injector;

	public StatsPushHandler(Injector injector)
	{
		this.injector = injector;
	}

	@Override
	public void configure(WebSocketServletFactory factory)
	{
		factory.setCreator(new WebSocketCreator()
		{
			public Object createWebSocket(UpgradeRequest req, UpgradeResponse resp)
			{
				StatsPushSocket statsPushSocket = injector.getInstance(StatsPushSocket.class);
				statsPushSocket.registerForServerStatUpdates();
				return statsPushSocket;
			}
		});
	}
}
