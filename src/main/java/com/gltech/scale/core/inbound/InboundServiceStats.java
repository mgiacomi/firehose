package com.gltech.scale.core.inbound;

import com.gltech.scale.monitoring.AvgStatOverTime;
import com.gltech.scale.monitoring.StatsManager;
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

		String groupName = "Inbound Service";
		this.addMessageSizeStat = statsManager.createAvgAndCountStat(groupName, "AddMessage.Size", "AddMessage.Count");
		this.addMessageTimeStat = statsManager.createAvgStat(groupName, "AddMessage.Time");
	}

	public void addMessage(String channelName, MediaType mediaTypes, byte[] payload)
	{
		addMessageTimeStat.startTimer();
		inboundService.addMessage(channelName, mediaTypes, payload);
		addMessageTimeStat.stopTimer();

		addMessageSizeStat.add(payload.length);
	}

	public int writeMessagesToOutputStream(String channelName, DateTime dateTime, OutputStream outputStream, int recordsWritten)
	{
		return inboundService.writeMessagesToOutputStream(channelName, dateTime, outputStream, recordsWritten);
	}

	public void shutdown()
	{
		inboundService.shutdown();
	}
}
