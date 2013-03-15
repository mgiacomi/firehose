package com.gltech.scale.core.storage;

import com.gltech.scale.core.model.ChannelMetaData;
import com.google.inject.Inject;
import com.gltech.scale.core.storage.bytearray.InvalidVersionException;
import org.apache.commons.io.IOUtils;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Path("/storage")
public class StorageResource
{
	@Context
	private UriInfo uriInfo = null;

	@Context
	private HttpHeaders httpHeaders = null;

	private Storage storage;
	private ChannelCache channelCache;

	@Inject
	public StorageResource(Storage storage, ChannelCache channelCache)
	{
		this.storage = storage;
		this.channelCache = channelCache;
	}

	@Path("/{customer}/{bucket}")
	@PUT
	public Response putBucket(@PathParam("customer") String customer,
							  @PathParam("bucket") String bucket,
							  String json)
	{
		try
		{
			if (customer.contains("|") || bucket.contains("|"))
			{
				return Response.status(Response.Status.BAD_REQUEST).entity("Customer and Bucket can not contain '|' aka pipes").build();
			}
//			ChannelMetaData channelMetaData = new ChannelMetaData(customer, bucket, json);
ChannelMetaData channelMetaData = null;
			storage.putBucket(channelMetaData);
			return Response.created(uriInfo.getRequestUri()).build();
		}
		catch (BucketMetaDataException e)
		{
			return Response.status(Response.Status.BAD_REQUEST).entity("unable to parse json input").build();
		}
		catch (DuplicateBucketException e)
		{
			return Response.status(Response.Status.FORBIDDEN).entity("Bucket " + bucket + " already exists").build();
		}
	}

	@Path("/{channelName}")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getBucket(@PathParam("channelName") String channelName)
	{
		ChannelMetaData channelMetaData = storage.getBucket(channelName);
		if (channelMetaData == null)
		{
			return Response.status(Response.Status.NOT_FOUND).build();
		}
//		return Response.ok(channelMetaData.toJson().toString()).build();
return null;
	}

	@Path("/{channelName}/{id}")
	@PUT
	public Response put(@PathParam("channelName") String channelName, @PathParam("id") String id, InputStream inputStream)
	{
		try
		{
			storage.putPayload(channelName, id, inputStream, httpHeaders.getRequestHeaders());
			IOUtils.closeQuietly(inputStream);
			return Response.created(uriInfo.getRequestUri()).build();
		}
		catch (InvalidVersionException e)
		{
			return Response.status(Response.Status.PRECONDITION_FAILED).build();
		}
	}

	@Path("/{channelName}/{id}")
	@GET
	public Response get(@PathParam("channelName") final String channelName, @PathParam("id") final String id)
	{
		StreamingOutput out = new StreamingOutput()
		{
			public void write(OutputStream outputStream) throws IOException, WebApplicationException
			{
//				storage.getPayload(customer, bucket, id, outputStream);
			}
		};

		ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, false);

		Response.ResponseBuilder builder = Response.ok(out);

		if (null != channelMetaData)
		{
//			builder.type(channelMetaData.getMediaType());
		}

		return builder.build();
	}

	//todo - gfm - 9/26/12 - should we return a payload's meta data?  from what url?
}
