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
	private BucketMetaDataCache bucketMetaDataCache;

	@Inject
	public StorageResource(Storage storage, BucketMetaDataCache bucketMetaDataCache)
	{
		this.storage = storage;
		this.bucketMetaDataCache = bucketMetaDataCache;
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
			ChannelMetaData channelMetaData = new ChannelMetaData(customer, bucket, json);
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

	@Path("/{customer}/{bucket}")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getBucket(@PathParam("customer") String customer,
							  @PathParam("bucket") String bucket)
	{
		ChannelMetaData channelMetaData = storage.getBucket(customer, bucket);
		if (channelMetaData == null)
		{
			return Response.status(Response.Status.NOT_FOUND).build();
		}
		return Response.ok(channelMetaData.toJson().toString()).build();
	}

	@Path("/{customer}/{bucket}/{id}")
	@PUT
	public Response put(@PathParam("customer") String customer,
						@PathParam("bucket") String bucket,
						@PathParam("id") String id,
						InputStream inputStream)
	{
		try
		{
			storage.putPayload(customer, bucket, id, inputStream, httpHeaders.getRequestHeaders());
			IOUtils.closeQuietly(inputStream);
			return Response.created(uriInfo.getRequestUri()).build();
		}
		catch (InvalidVersionException e)
		{
			return Response.status(Response.Status.PRECONDITION_FAILED).build();
		}
	}

	@Path("/{customer}/{bucket}/{id}")
	@GET
	public Response get(@PathParam("customer") final String customer,
						@PathParam("bucket") final String bucket,
						@PathParam("id") final String id)
	{
		StreamingOutput out = new StreamingOutput()
		{
			public void write(OutputStream outputStream) throws IOException, WebApplicationException
			{
				storage.getPayload(customer, bucket, id, outputStream);
			}
		};

		ChannelMetaData channelMetaData = bucketMetaDataCache.getBucketMetaData(customer, bucket, false);

		Response.ResponseBuilder builder = Response.ok(out);

		if (null != channelMetaData)
		{
			builder.type(channelMetaData.getMediaType());
		}

		return builder.build();
	}

	//todo - gfm - 9/26/12 - should we return a payload's meta data?  from what url?
}
