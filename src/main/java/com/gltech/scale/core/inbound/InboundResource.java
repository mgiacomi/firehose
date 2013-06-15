package com.gltech.scale.core.inbound;

import com.gltech.scale.core.storage.DuplicateChannelException;
import com.gltech.scale.core.storage.StorageClient;
import com.gltech.scale.core.model.ModelIO;
import com.google.inject.Inject;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.ChannelCache;
import com.gltech.scale.util.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.*;

@Path("/inbound")
public class InboundResource
{
	private static final Logger logger = LoggerFactory.getLogger(InboundResource.class);
	private ChannelCache channelCache;
	private InboundService inboundService;
	private ModelIO modelIO;
	private StorageClient storageClient;
	private static Props props = Props.getProps();

	@Context
	private HttpHeaders httpHeaders;

	@Context
	private UriInfo uriInfo;

	@Inject
	public InboundResource(ChannelCache channelCache, InboundService inboundService, ModelIO modelIO, StorageClient storageClient)
	{
		this.channelCache = channelCache;
		this.inboundService = inboundService;
		this.modelIO = modelIO;
		this.storageClient = storageClient;
	}

	@POST
	@Path("/{channelName}")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response postMessage(@PathParam("channelName") String channelName, byte[] payload)
	{
		inboundService.addMessage(channelName, httpHeaders.getMediaType(), uriInfo.getRequestUri().getQuery(), payload);
		return Response.status(Response.Status.ACCEPTED).build();
	}

	@PUT
	@Path("/channel/new")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response putChannel(byte[] payload)
	{
		ChannelMetaData channelMetaData = modelIO.toChannelMetaData(new String(payload));

		if (channelMetaData.getName() == null || channelMetaData.getTtl() == null)
		{
			throw new IllegalStateException("JSON to create channel was invalid: " + new String(payload));
		}

		try
		{
			storageClient.putChannelMetaData(channelMetaData);
			return Response.status(Response.Status.CREATED).build();
		}
		catch (DuplicateChannelException e)
		{
			String msg = "A channel with this name has already been created.";
			return Response.status(Response.Status.CONFLICT).entity(msg).type(MediaType.TEXT_PLAIN).build();
		}
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
}
