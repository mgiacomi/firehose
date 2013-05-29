package com.gltech.scale.core.server;

import com.gltech.scale.util.ClientCreator;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import javax.ws.rs.core.MediaType;

public class LoadClient
{
	static private final Client client = ClientCreator.createCached();

	public static void main(String[] args) throws Exception
	{
		int counter = 0;
		while (true)
		{
			postEvent("http://localhost:" + args[0], "fast", String.valueOf(counter + "a"));
			postEvent("http://localhost:" + args[0], "redundant", String.valueOf(counter + "b"));
			counter++;
		}
	}

	static private void postEvent(String url, String channelName, String json)
	{
		WebResource webResource = client.resource(url + "/inbound/" + channelName);
		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON_TYPE).post(ClientResponse.class, json);

		if (response.getStatus() != 202)
		{
			throw new RuntimeException("Failed : HTTP error code: " + response.getStatus());
		}
	}
}