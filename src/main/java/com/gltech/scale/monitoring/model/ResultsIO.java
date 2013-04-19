package com.gltech.scale.monitoring.model;

import com.dyuproject.protostuff.JsonIOUtil;
import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;

import java.io.*;
import java.util.List;

public class ResultsIO
{
	private Schema<ServerStats> serverStatsSchema = RuntimeSchema.getSchema(ServerStats.class);
	private ObjectMapper mapper = new ObjectMapper();

	public byte[] toBytes(ServerStats serverStats)
	{
		LinkedBuffer linkedBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
		return ProtostuffIOUtil.toByteArray(serverStats, serverStatsSchema, linkedBuffer);
	}

	public String toJson(ServerStats serverStats)
	{
		if (serverStats == null)
		{
			return "{}";
		}

		try
		{
			StringWriter writer = new StringWriter();
			mapper.writeValue(writer, serverStats);
			return writer.toString();
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public String toJson(ClusterStats clusterStats)
	{
		if (clusterStats == null)
		{
			return "{}";
		}

		try
		{
			StringWriter writer = new StringWriter();
			mapper.writeValue(writer, clusterStats);
			return writer.toString();
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public byte[] toBytes(List<ServerStats> serverStatsList)
	{
		try
		{
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			LinkedBuffer linkedBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
			ProtostuffIOUtil.writeListTo(out, serverStatsList, serverStatsSchema, linkedBuffer);
			return out.toByteArray();
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public String toJson(List<ServerStats> serverStatsList)
	{
		if (serverStatsList == null)
		{
			return "{}";
		}

		try
		{
			StringWriter writer = new StringWriter();
			mapper.writeValue(writer, serverStatsList);
			return writer.toString();
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public ServerStats toServerStats(byte[] bytes)
	{
		ServerStats serverStats = new ServerStats();
		ProtostuffIOUtil.mergeFrom(bytes, serverStats, serverStatsSchema);

		if (serverStats.getWorkerId() == null)
		{
			throw new IllegalArgumentException("The byte[] supplied was not a Protostuff binary array.");
		}

		return serverStats;
	}

	public ServerStats toServerStats(String json)
	{
		try
		{
			ServerStats serverStats = new ServerStats();
			JsonIOUtil.mergeFrom(json.getBytes(), serverStats, serverStatsSchema, false);
			return serverStats;
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public List<ServerStats> toServerStatsList(byte[] bytes)
	{
		try
		{
			ByteArrayInputStream in = new ByteArrayInputStream(bytes);
			return ProtostuffIOUtil.parseListFrom(in, serverStatsSchema);
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public List<ServerStats> toServerStatsList(String json)
	{
		try
		{
			StringReader reader = new StringReader(json);
			return JsonIOUtil.parseListFrom(reader, serverStatsSchema, false);
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}
}
