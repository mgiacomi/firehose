package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.BatchMetaData;
import com.gltech.scale.core.stats.StatsManager;
import com.gltech.scale.core.model.ModelIO;
import com.google.inject.Inject;
import org.joda.time.DateTime;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;

@Path("/aggregator")
public class AggregatorResource
{
	private Aggregator aggregator;
	private ModelIO modelIO;

	@Inject
	public AggregatorResource(Aggregator aggregator, ModelIO modelIO)
	{
		this.aggregator = aggregator;
		this.modelIO = modelIO;
	}

	@POST
	@Path("/message/{channelName}/{year}/{month}/{day}/{hour}/{min}/{sec}")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response postMessage(@PathParam("channelName") String channelName, @PathParam("year") int year, @PathParam("month") int month, @PathParam("day") int day,
								@PathParam("hour") int hour, @PathParam("min") int min, @PathParam("sec") int sec, byte[] data)
	{
		DateTime dateTime = new DateTime(year, month, day, hour, min, sec);
		aggregator.addMessage(channelName, data, dateTime);
		return Response.status(Response.Status.ACCEPTED).build();
	}

	@POST
	@Path("/backup/message/{channelName}/{year}/{month}/{day}/{hour}/{min}/{sec}")
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response postBackupMessage(@PathParam("channelName") String channelName, @PathParam("year") int year, @PathParam("month") int month, @PathParam("day") int day,
									  @PathParam("hour") int hour, @PathParam("min") int min, @PathParam("sec") int sec, byte[] data)
	{
		DateTime dateTime = new DateTime(year, month, day, hour, min, sec);
		aggregator.addBackupMessage(channelName, data, dateTime);
		return Response.status(Response.Status.ACCEPTED).build();
	}

	@DELETE
	@Path("/{channelName}/{year}/{month}/{day}/{hour}/{min}/{sec}/clear")
	public Response clearBatch(@PathParam("channelName") String channelName, @PathParam("year") int year, @PathParam("month") int month,
							   @PathParam("day") int day, @PathParam("hour") int hour, @PathParam("min") int min, @PathParam("sec") int sec)
	{
		DateTime dateTime = new DateTime(year, month, day, hour, min, sec);
		aggregator.clear(channelName, dateTime);

		return Response.ok().build();
	}

	@GET
	@Path("/{channelName}/{year}/{month}/{day}/{hour}/{min}/{sec}/messages")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response batchMessages(@PathParam("channelName") final String channelName, @PathParam("year") int year, @PathParam("month") int month,
								  @PathParam("day") int day, @PathParam("hour") int hour, @PathParam("min") int min, @PathParam("sec") int sec)
	{
		final DateTime dateTime = new DateTime(year, month, day, hour, min, sec);

		StreamingOutput out = new StreamingOutput()
		{
			public void write(OutputStream output) throws IOException, WebApplicationException
			{
				try
				{
					aggregator.writeBatchMessages(output, channelName, dateTime);
				}
				catch (Exception e)
				{
					throw new WebApplicationException(e);
				}
			}
		};

		return Response.ok(out, MediaType.APPLICATION_JSON_TYPE).build();
	}

	@GET
	@Path("/{channelName}/{year}/{month}/{day}/{hour}/{min}/{sec}/backup/messages")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response backupBatchMessages(@PathParam("channelName") final String channelName, @PathParam("year") int year, @PathParam("month") int month,
										@PathParam("day") int day, @PathParam("hour") int hour, @PathParam("min") int min, @PathParam("sec") int sec)
	{
		final DateTime dateTime = new DateTime(year, month, day, hour, min, sec);

		StreamingOutput out = new StreamingOutput()
		{
			public void write(OutputStream output) throws IOException, WebApplicationException
			{
				try
				{
					aggregator.writeBackupBatchMessages(output, channelName, dateTime);
				}
				catch (Exception e)
				{
					throw new WebApplicationException(e);
				}
			}
		};

		return Response.ok(out, MediaType.APPLICATION_JSON_TYPE).build();
	}

	@GET
	@Path("/{channelName}/batch/{year}/{month}/{day}/{hour}/{min}/{sec}/metadata")
	@Produces(MediaType.APPLICATION_JSON)
	public Response batchMetaData(@PathParam("channelName") String channelName, @PathParam("year") int year, @PathParam("month") int month,
								  @PathParam("day") int day, @PathParam("hour") int hour, @PathParam("min") int min, @PathParam("sec") int sec)
	{
		DateTime dateTime = new DateTime(year, month, day, hour, min, sec);

		BatchMetaData batchMetaData = aggregator.getBatchMetaData(channelName, dateTime);

		return Response.ok(modelIO.toJson(batchMetaData), MediaType.APPLICATION_JSON).build();
	}

	@GET
	@Path("/{channelName}/backup/batch/{year}/{month}/{day}/{hour}/{min}/{sec}/metadata")
	@Produces(MediaType.APPLICATION_JSON)
	public Response backupBatchMetaData(@PathParam("channelName") String channelName, @PathParam("year") int year, @PathParam("month") int month,
										@PathParam("day") int day, @PathParam("hour") int hour, @PathParam("min") int min, @PathParam("sec") int sec)
	{
		DateTime dateTime = new DateTime(year, month, day, hour, min, sec);

		BatchMetaData batchMetaData = aggregator.getBatchBucketMetaData(channelName, dateTime);

		return Response.ok(modelIO.toJson(batchMetaData), MediaType.APPLICATION_JSON).build();
	}
}
