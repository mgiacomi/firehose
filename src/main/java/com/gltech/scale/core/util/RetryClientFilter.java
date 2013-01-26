package com.gltech.scale.core.util;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.ClientFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;

public class RetryClientFilter extends ClientFilter
{
	private static final Logger logger = LoggerFactory.getLogger(RetryClientFilter.class);

	public ClientResponse handle(ClientRequest clientRequest) throws ClientHandlerException
	{

		int maxRetries = Props.getProps().get("RetryClientFilter.MaxRetries", 3);
		int sleep = Props.getProps().get("RetryClientFilter.SleepMillis", 1000);

		ClientHandlerException lastCause = null;
		int i = 0;

		while (i < maxRetries)
		{

			i++;
			try
			{
				return getNext().handle(clientRequest);
			}
			catch (ClientHandlerException e)
			{
				if (e.getCause() == null)
				{
					throw e;
				}
				if (UnknownHostException.class.isAssignableFrom(e.getCause().getClass()))
				{
					throw e;
				}
				lastCause = e;

				logger.info("exception {} retry count {} ", clientRequest.getURI().toString(), i);
				logger.debug(clientRequest.getURI().toString() + " stacktrace ", e);
				ThreadSleep.sleep(sleep);
			}
		}
		String msg = "Connection retries limit " + maxRetries + " exceeded for uri " + clientRequest.getURI();
		logger.warn(msg);
		throw new ClientHandlerException(msg, lastCause);
	}

}
