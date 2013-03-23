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
	private AvgStatOverTime addMessageStat;

	@Inject
	public InboundServiceStats(@Named(BASE) final InboundService inboundService, StatsManager statsManager)
	{
		this.inboundService = inboundService;
		this.addMessageStat = statsManager.createAvgStat("Inbound Service", "Message.Size");
	}

	public void addMessage(String channelName, MediaType mediaTypes, byte[] payload)
	{
		addMessageStat.add(payload.length);
		inboundService.addMessage(channelName, mediaTypes, payload);
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
