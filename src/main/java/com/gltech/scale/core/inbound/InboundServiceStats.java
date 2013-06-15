package com.gltech.scale.core.inbound;

import com.gltech.scale.core.stats.AvgStatOverTime;
import com.gltech.scale.core.stats.StatsManager;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.joda.time.DateTime;

import javax.ws.rs.core.MediaType;
import java.io.OutputStream;

public class InboundServiceStats implements InboundService
{
	public static final String BASE = "InboundServiceStats";
	private final InboundService inboundService;
	private AvgStatOverTime addMessageSizeStat;
	private AvgStatOverTime addMessageTimeStat;

	@Inject
	public InboundServiceStats(@Named(BASE) final InboundService inboundService, StatsManager statsManager)
	{
		this.inboundService = inboundService;

		String groupName = "Inbound";
		this.addMessageSizeStat = statsManager.createAvgStat(groupName, "AddMessage_Size", "bytes");
		this.addMessageSizeStat.activateCountStat("AddMessage_Count", "messages");
		this.addMessageTimeStat = statsManager.createAvgStat(groupName, "AddMessage_Time", "milliseconds");
	}

	@Override
	public void addMessage(String channelName, MediaType mediaTypes, String queryString, byte[] payload)
	{
		addMessageTimeStat.startTimer();
		inboundService.addMessage(channelName, mediaTypes, queryString, payload);
		addMessageTimeStat.stopTimer();

		addMessageSizeStat.add(payload.length);
	}

	@Override
	public void shutdown()
	{
		inboundService.shutdown();
	}
}
