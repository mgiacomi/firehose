package com.gltech.scale.monitoring.server;

import com.gltech.scale.core.stats.results.ResultsIO;
import com.gltech.scale.core.stats.results.ServerStats;
import com.gltech.scale.monitoring.services.ClusterStatsCallBack;
import com.gltech.scale.monitoring.services.ClusterStatsService;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

@WebSocket
public class StatsPushSocket implements ClusterStatsCallBack
{
	private static final Logger logger = LoggerFactory.getLogger(StatsPushSocket.class);
	private final Set<Session> sessions = new CopyOnWriteArraySet<>();
	private ClusterStatsService clusterStatsService;
	private ResultsIO resultsIO;

	@Inject
	public StatsPushSocket(ClusterStatsService clusterStatsService, ResultsIO resultsIO)
	{
		this.clusterStatsService = clusterStatsService;
		this.resultsIO = resultsIO;
	}

	public void registerForServerStatUpdates()
	{
		clusterStatsService.registerCallback(this);
	}

	@Override
	public void serverStatsUpdate(ServerStats serverStats)
	{
		for (Session session : sessions)
		{
			// send a message to the current client WebSocket.
			try
			{
				session.getRemote().sendString(resultsIO.toJson(serverStats));
			}
			catch (IOException e)
			{
				logger.info("Failed to write to session {}", session.toString());
			}
		}
	}

	@OnWebSocketConnect
	public void onConnect(Session session)
	{
		logger.info("Adding session {}", session);
		sessions.add(session);
	}

	@OnWebSocketMessage
	public void onText(Session session, String message)
	{
		System.out.println("WEBSOCKET MESSAGE: "+ message);
	}

	@OnWebSocketClose
	public void onClose(Session session, int statusCode, String reason)
	{
		logger.info("Removing session statusCode={}, reason={} session={}", statusCode, reason, session);
		sessions.remove(session);
	}

	@OnWebSocketError
	public void onError(Session session, Throwable error)
	{
		System.out.println("WEBSOCKET ERROR");
	}

}
