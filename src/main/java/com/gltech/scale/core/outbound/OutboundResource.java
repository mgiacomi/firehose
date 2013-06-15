package com.gltech.scale.core.outbound;

import com.gltech.scale.core.inbound.InboundService;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.model.ModelIO;
import com.gltech.scale.core.storage.ChannelCache;
import com.gltech.scale.core.storage.StorageClient;
import com.gltech.scale.util.Props;
import com.google.inject.Inject;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.io.OutputStream;

@Path("/outbound")
public class OutboundResource
{
	private static final Logger logger = LoggerFactory.getLogger(OutboundResource.class);
	private OutboundService outboundService;
	private ChannelCache channelCache;
	private ModelIO modelIO;
	private static Props props = Props.getProps();

	@Context
	private HttpHeaders httpHeaders;

	@Context
	private UriInfo uriInfo;

	@Inject
	public OutboundResource(ChannelCache channelCache, ModelIO modelIO, OutboundService outboundService)
	{
		this.channelCache = channelCache;
		this.modelIO = modelIO;
		this.outboundService = outboundService;
	}

	@GET
	@Path("/channel/{channelName}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getChannel(@PathParam("channelName") String channelName)
	{
		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, false);

		if (channelMetaData == null)
		{
			return Response.status(Response.Status.NOT_FOUND).build();
		}

		return Response.ok(modelIO.toJson(channelMetaData), MediaType.APPLICATION_JSON).build();
	}

	@GET
	@Path("/{channelName}/{year}/{month}/{day}/{hour}/{min}/{sec}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response get(@PathParam("channelName") String channelName, @PathParam("year") int year, @PathParam("month") int month,
						@PathParam("day") int day, @PathParam("hour") int hour, @PathParam("min") int min, @PathParam("sec") int sec)
	{
		StreamingOutput streamingOutput = outboundService.getMessages(channelName, year, month, day, hour, min, sec);
		return Response.ok(streamingOutput, MediaType.APPLICATION_JSON).build();
	}

	@GET
	@Path("/{channelName}/{year}/{month}/{day}/{hour}/{min}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response get(@PathParam("channelName") String channelName, @PathParam("year") int year, @PathParam("month") int month,
						@PathParam("day") int day, @PathParam("hour") int hour, @PathParam("min") int min)
	{
		StreamingOutput streamingOutput = outboundService.getMessages(channelName, year, month, day, hour, min, -1);
		return Response.ok(streamingOutput, MediaType.APPLICATION_JSON).build();
	}

	@GET
	@Path("/{channelName}/{year}/{month}/{day}/{hour}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response get(@PathParam("channelName") String channelName, @PathParam("year") int year, @PathParam("month") int month,
						@PathParam("day") int day, @PathParam("hour") int hour)
	{
		StreamingOutput streamingOutput = outboundService.getMessages(channelName, year, month, day, hour, -1, -1);
		return Response.ok(streamingOutput, MediaType.APPLICATION_JSON).build();
	}
}
