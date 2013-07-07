package com.gltech.scale.core.aggregator;

import com.google.inject.Injector;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.eclipse.jetty.websocket.api.UpgradeResponse;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

public class AggregatorSocketHandler extends WebSocketHandler
{
	private Injector injector;

	public AggregatorSocketHandler(Injector injector)
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
				return injector.getInstance(AggregatorSocket.class);
			}
		});
	}
}
