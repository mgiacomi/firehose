package com.gltech.scale.core.inbound;

import javax.ws.rs.*;
import javax.ws.rs.core.*;

@Path("/inbound/monitor")
public class InboundMonitorResource
{
	@Context
	private HttpHeaders httpHeaders;

	@Context
	UriInfo uriInfo;

	//@Inject
	public InboundMonitorResource()
	{
	}

	@GET
	@Path("/rates")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getRates()
	{
//		return Response.ok(modelIO.toJson(channelMetaData), MediaType.APPLICATION_JSON).build();
		return Response.ok().build();
	}

}
