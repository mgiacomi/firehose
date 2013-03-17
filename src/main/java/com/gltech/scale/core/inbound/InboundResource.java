package com.gltech.scale.core.inbound;

import com.gltech.scale.core.model.Defaults;
import com.google.inject.Inject;
import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.ChannelCache;
import com.gltech.scale.util.Http404Exception;
import com.gltech.scale.util.Props;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.io.OutputStream;

@Path("/events")
public class InboundResource
{
	private static final Logger logger = LoggerFactory.getLogger(InboundResource.class);
	private ChannelCache channelCache;
	private InboundService inboundService;
	private static Props props = Props.getProps();
	private int periodSeconds;

	@Context
	private HttpHeaders httpHeaders;

	@Context
	UriInfo uriInfo;

	@Inject
	public InboundResource(ChannelCache channelCache, InboundService inboundService)
	{
		this.channelCache = channelCache;
		this.inboundService = inboundService;
		this.periodSeconds = props.get("period_seconds", Defaults.PERIOD_SECONDS);
	}

	@POST
	@Path("/{channelName}")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response post(@PathParam("channelName") String channelName, byte[] payload)
	{
		inboundService.addEvent(channelName, httpHeaders.getMediaType(), payload);
		return Response.status(Response.Status.ACCEPTED).build();
	}

	@GET
	@Path("/{channelName}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response post(@PathParam("channelName") String channelName)
	{
		try
		{
			ChannelMetaData channelMetaData = channelCache.getChannelMetaData(channelName, false);
//			return Response.ok(channelMetaData.toJson().toString(), MediaType.APPLICATION_JSON).build();
return null;
		}
		catch (Http404Exception e)
		{
			return Response.status(404).build();

		}
	}

	@GET
	@Path("/{channelName}/{year}/{month}/{day}/{hour}/{min}/{sec}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response get(@PathParam("channelName") String channelName, @PathParam("year") int year, @PathParam("month") int month,
						@PathParam("day") int day, @PathParam("hour") int hour, @PathParam("min") int min, @PathParam("sec") int sec)
	{
		return getEventsOrRedirect(channelName, year, month, day, hour, min, sec);
	}

	@GET
	@Path("/{channelName}/{year}/{month}/{day}/{hour}/{min}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response get(@PathParam("channelName") String channelName, @PathParam("year") int year, @PathParam("month") int month,
						@PathParam("day") int day, @PathParam("hour") int hour, @PathParam("min") int min)
	{
		return getEventsOrRedirect(channelName, year, month, day, hour, min, -1);
	}

	@GET
	@Path("/{channelName}/{year}/{month}/{day}/{hour}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response get(@PathParam("channelName") String channelName, @PathParam("year") int year,
						@PathParam("month") int month, @PathParam("day") int day, @PathParam("hour") int hour)
	{
		return getEventsOrRedirect(channelName, year, month, day, hour, -1, -1);
	}

	@GET
	@Path("/{channelName}/{year}/{month}/{day}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response get(@PathParam("channelName") String channelName, @PathParam("year") int year,
						@PathParam("month") int month, @PathParam("day") int day)
	{
		return getEventsOrRedirect(channelName, year, month, day, -1, -1, -1);
	}

	// Right now we only support getting events by second, minute, or hour.
	Response getEventsOrRedirect(final String channelName, final int year, final int month, final int day, final int hour, final int min, final int sec)
	{
		try
		{
			StreamingOutput out = new StreamingOutput()
			{
				public void write(OutputStream outputStream) throws IOException, WebApplicationException
				{
					outputStream.write("[".getBytes());

					try
					{
						int recordsWritten = 0;

						// If our period is less the 60 and a time with seconds was requested, then make a single request.
						if (sec > -1)
						{
							recordsWritten = inboundService.writeEventsToOutputStream(channelName, new DateTime(year, month, day, hour, min, sec), outputStream, recordsWritten);
						}

						// If our period is less the 60 and a time with no secs, but minutes was requested, then query all periods for this minute.
						else if (min > -1)
						{
							for (int i = 0; i < 60; i = i + periodSeconds)
							{
								DateTime dateTime = new DateTime(year, month, day, hour, min, i);

								if (dateTime.isBeforeNow())
								{
									recordsWritten = inboundService.writeEventsToOutputStream(channelName, dateTime, outputStream, recordsWritten);
								}
							}
						}

						// If our period is less the 60 and a time with no mins, but hours were requested, then query all periods for this hour.
						else if (hour > -1)
						{
							for (int m = 0; m < 60; m++)
							{
								for (int s = 0; s < 60; s = s + periodSeconds)
								{
									DateTime dateTime = new DateTime(year, month, day, hour, m, s);

									if (dateTime.isBeforeNow())
									{
										recordsWritten = inboundService.writeEventsToOutputStream(channelName, dateTime, outputStream, recordsWritten);
									}
								}
							}
						}

						// If our period is less the 60 and a time with no days, but minutes was requested, then query all periods for this day.
						else if (day > -1)
						{
							for (int h = 0; h < 23; h++)
							{
								for (int m = 0; m < 60; m++)
								{
									for (int s = 0; s < 60; s = s + periodSeconds)
									{
										DateTime dateTime = new DateTime(year, month, day, h, m, s);
										if (dateTime.isBeforeNow())
										{
											recordsWritten = inboundService.writeEventsToOutputStream(channelName, dateTime, outputStream, recordsWritten);
										}
									}
								}
							}
						}

						logger.debug("Wrote out {} records for channelName={} year={} month={} day={} hour={} min={} sec={}", recordsWritten, channelName, year, month, day, hour, min, sec);
					}
					catch (Exception e)
					{
						throw new WebApplicationException(e);
					}

					outputStream.write("]".getBytes());
				}
			};

/*
			if (channelMetaData.getMediaType().equals(MediaType.APPLICATION_JSON_TYPE))
			{
				return Response.ok(out, MediaType.APPLICATION_JSON).build();
			}
*/
		}
		catch (Http404Exception e)
		{
			return Response.status(404).build();
		}

		// We don't know how to handle your request.
		return Response.status(501).build();
	}
}
