package com.gltech.scale.core.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

public class Message implements Comparable<Message>
{
	private static final Logger logger = LoggerFactory.getLogger(Message.class);
	private static final ObjectMapper mapper = new ObjectMapper();
	private final String customer;
	private final String bucket;
	private final DateTime received_at;
	private final byte[] payload;
	private final String uuid;
	private final boolean stored;

	public Message(String customer, String bucket)
	{
		this.uuid = UUID.randomUUID().toString();
		this.received_at = DateTime.now();
		this.payload = new byte[0];
		this.customer = customer;
		this.bucket = bucket;
		this.stored = true;
	}

	public Message(String customer, String bucket, byte[] payload)
	{
		this.uuid = UUID.randomUUID().toString();
		this.received_at = DateTime.now();
		this.payload = payload;
		this.customer = customer;
		this.bucket = bucket;
		this.stored = false;
	}

	public Message(String customer, String bucket, DateTime received_at, byte[] payload, String uuid, boolean stored)
	{
		this.customer = customer;
		this.bucket = bucket;
		this.received_at = received_at;
		this.payload = payload;
		this.uuid = uuid;
		this.stored = stored;
	}

	public Message(String json)
	{
		try
		{
			JsonNode rootNode = mapper.readTree(json);
			this.customer = rootNode.path("customer").asText();
			this.bucket = rootNode.path("bucket").asText();
			this.uuid = rootNode.path("uuid").asText();
			this.payload = rootNode.path("payload").binaryValue();
			this.received_at = new DateTime(rootNode.path("received_at").asText());
			this.stored = rootNode.path("stored").booleanValue();
		}
		catch (IOException e)
		{
			logger.warn("unable to parse json {}", json, e);
			throw new RuntimeException("unable to parse json " + json, e);
		}
	}

	public JsonNode toJson()
	{
		ObjectNode objectNode = mapper.createObjectNode();
		objectNode.put("customer", customer);
		objectNode.put("bucket", bucket);
		objectNode.put("uuid", uuid);
		objectNode.put("received_at", received_at.toString());
		objectNode.put("payload", BinaryNode.valueOf(payload));
		objectNode.put("stored", stored);

		return objectNode;
	}

	static public Message jsonToEvent(JsonParser jp)
	{
		try
		{
			jp.nextToken();
			String customer = null;
			String bucket = null;
			String uuid = null;
			DateTime received_at = null;
			byte[] payload = null;
			boolean stored = false;

			do
			{
				jp.nextToken();

				String payloadFieldName = jp.getCurrentName();

				if ("customer".equalsIgnoreCase(payloadFieldName))
				{
					customer = jp.getText();
				}
				else if ("bucket".equalsIgnoreCase(payloadFieldName))
				{
					bucket = jp.getText();
				}
				else if ("uuid".equalsIgnoreCase(payloadFieldName))
				{
					uuid = jp.getText();
				}
				else if ("received_at".equalsIgnoreCase(payloadFieldName))
				{
					received_at = new DateTime(jp.getText());
				}
				else if ("payload".equalsIgnoreCase(payloadFieldName))
				{
					payload = jp.getBinaryValue();
				}
				else if ("stored".equalsIgnoreCase(payloadFieldName))
				{
					stored = jp.getBooleanValue();
				}
				else
				{
					throw new IllegalStateException("Unrecognized field for EventPayload '" + payloadFieldName + "'! token=" + jp.getCurrentToken() + ", customer=" + customer + ", bucket=" + bucket + ", uuid=" + uuid + ", received_at=" + received_at);
				}
			} while (jp.nextToken() != JsonToken.END_OBJECT);

			return new Message(customer, bucket, received_at, payload, uuid, stored);
		}
		catch (IOException e)
		{
			logger.warn("unable to parse json", e);
			throw new RuntimeException("unable to parse json", e);
		}
	}

	public String getId()
	{
		return received_at.toString("YYYY-MM-dd-HH-mm-ss");
	}

	public String getCustomer()
	{
		return customer;
	}

	public String getBucket()
	{
		return bucket;
	}

	public DateTime getReceived_at()
	{
		return received_at;
	}

	public byte[] getPayload()
	{
		return payload;
	}

	public String getUuid()
	{
		return uuid;
	}

	public boolean isStored()
	{
		return stored;
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Message that = (Message) o;

		if (bucket != null ? !bucket.equals(that.bucket) : that.bucket != null) return false;
		if (customer != null ? !customer.equals(that.customer) : that.customer != null) return false;
		if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) return false;

		return true;
	}

	public int hashCode()
	{
		int result = customer != null ? customer.hashCode() : 0;
		result = 31 * result + (bucket != null ? bucket.hashCode() : 0);
		result = 31 * result + (uuid != null ? uuid.hashCode() : 0);
		return result;
	}

	public int compareTo(Message o)
	{
		int compare = this.getReceived_at().compareTo(o.getReceived_at());

		if (compare != 0)
		{
			return compare;
		}

		return this.getUuid().compareTo(o.getUuid());
	}
}
