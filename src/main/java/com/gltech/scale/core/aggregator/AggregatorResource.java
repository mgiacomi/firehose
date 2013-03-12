package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.model.Message;
import com.google.inject.Inject;
import com.gltech.scale.util.Http404Exception;
import org.joda.time.DateTime;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;

@Path("/ropes")
public class AggregatorResource
{
	private Aggregator aggregator;

	@Inject
	public AggregatorResource(Aggregator aggregator)
	{
		this.aggregator = aggregator;
	}

	@Path("/event")
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response postEvent(byte[] data)
	{
		Message event = new Message(new String(data));
		aggregator.addEvent(event);
		return Response.status(Response.Status.ACCEPTED).build();
	}

	@Path("/backup/event")
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response postBackupEvent(byte[] data)
	{
		Message event = new Message(new String(data));
		aggregator.addBackupEvent(event);
		return Response.status(Response.Status.ACCEPTED).build();
	}

	@Path("/{customer}/{bucket}/{year}/{month}/{day}/{hour}/{min}/{sec}/clear")
	@DELETE
	public Response clearTimeBucket(@PathParam("customer") String customer, @PathParam("bucket") String bucket, @PathParam("year") int year, @PathParam("month") int month,
									@PathParam("day") int day, @PathParam("hour") int hour, @PathParam("min") int min, @PathParam("sec") int sec)
	{
		DateTime dateTime = new DateTime(year, month, day, hour, min, sec);
		aggregator.clear(customer, bucket, dateTime);

		return Response.ok().build();
	}

	@Path("/{customer}/{bucket}/{year}/{month}/{day}/{hour}/{min}/{sec}/timebucket/events")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response timeBucketEvents(@PathParam("customer") final String customer, @PathParam("bucket") final String bucket, @PathParam("year") int year, @PathParam("month") int month,
									 @PathParam("day") int day, @PathParam("hour") int hour, @PathParam("min") int min, @PathParam("sec") int sec)
	{
		final DateTime dateTime = new DateTime(year, month, day, hour, min, sec);

		try
		{
			StreamingOutput out = new StreamingOutput()
			{
				public void write(OutputStream output) throws IOException, WebApplicationException
				{
					try
					{
						aggregator.writeTimeBucketEvents(output, customer, bucket, dateTime);
					}
					catch (Exception e)
					{
						throw new WebApplicationException(e);
					}
				}
			};

			return Response.ok(out, MediaType.APPLICATION_JSON_TYPE).build();
		}
		catch (Http404Exception e)
		{
			return Response.status(404).build();
		}
	}

	@Path("/{customer}/{bucket}/{year}/{month}/{day}/{hour}/{min}/{sec}/backup/timebucket/events")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response backupTimeBucketEvents(@PathParam("customer") final String customer, @PathParam("bucket") final String bucket, @PathParam("year") int year, @PathParam("month") int month,
										   @PathParam("day") int day, @PathParam("hour") int hour, @PathParam("min") int min, @PathParam("sec") int sec)
	{
		final DateTime dateTime = new DateTime(year, month, day, hour, min, sec);

		try
		{
			StreamingOutput out = new StreamingOutput()
			{
				public void write(OutputStream output) throws IOException, WebApplicationException
				{
					try
					{
						aggregator.writeBackupTimeBucketEvents(output, customer, bucket, dateTime);
					}
					catch (Exception e)
					{
						throw new WebApplicationException(e);
					}
				}
			};

			return Response.ok(out, MediaType.APPLICATION_JSON_TYPE).build();
		}
		catch (Http404Exception e)
		{
			return Response.status(404).build();
		}
	}

	@Path("/{customer}/{bucket}/timebucket/{year}/{month}/{day}/{hour}/{min}/{sec}/metadata")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response timeBucketMetaData(@PathParam("customer") String customer, @PathParam("bucket") String bucket, @PathParam("year") int year, @PathParam("month") int month,
									   @PathParam("day") int day, @PathParam("hour") int hour, @PathParam("min") int min, @PathParam("sec") int sec)
	{
		DateTime dateTime = new DateTime(year, month, day, hour, min, sec);

		try
		{
			BatchMetaData batchMetaData = aggregator.getTimeBucketMetaData(customer, bucket, dateTime);

			return Response.ok(batchMetaData.toJson().toString(), MediaType.APPLICATION_JSON).build();
		}
		catch (Http404Exception e)
		{
			return Response.status(404).build();
		}
	}

	@Path("/{customer}/{bucket}/backup/timebucket/{year}/{month}/{day}/{hour}/{min}/{sec}/metadata")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response backupTimeBucketMetaData(@PathParam("customer") String customer, @PathParam("bucket") String bucket, @PathParam("year") int year, @PathParam("month") int month,
											 @PathParam("day") int day, @PathParam("hour") int hour, @PathParam("min") int min, @PathParam("sec") int sec)
	{
		DateTime dateTime = new DateTime(year, month, day, hour, min, sec);

		try
		{
			BatchMetaData batchMetaData = aggregator.getBackupTimeBucketMetaData(customer, bucket, dateTime);

			return Response.ok(batchMetaData.toJson().toString(), MediaType.APPLICATION_JSON).build();
		}
		catch (Http404Exception e)
		{
			return Response.status(404).build();
		}
	}
}
