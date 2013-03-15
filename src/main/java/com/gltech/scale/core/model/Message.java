package com.gltech.scale.core.model;

import com.dyuproject.protostuff.Tag;
import org.joda.time.DateTime;

import java.util.UUID;


public class Message
{
	@Tag(1)
	private final String uuid;
	@Tag(2)
	private final int mimeType;
	@Tag(3)
	private final byte[] payload;
	@Tag(4)
	private final long received_at;
	@Tag(5)
	private final boolean stored;

	public final static String APPLICATION_XML = "application/xml";
	public final static int APPLICATION_XML_TYPE = 1;
	public final static String APPLICATION_ATOM_XML = "application/atom+xml";
	public final static int APPLICATION_ATOM_XML_TYPE = 2;
	public final static String APPLICATION_XHTML_XML = "application/xhtml+xml";
	public final static int APPLICATION_XHTML_XML_TYPE = 3;
	public final static String APPLICATION_SVG_XML = "application/svg+xml";
	public final static int APPLICATION_SVG_XML_TYPE = 4;
	public final static String APPLICATION_JSON = "application/json";
	public final static int APPLICATION_JSON_TYPE = 5;
	public final static String APPLICATION_FORM_URLENCODED = "application/x-www-form-urlencoded";
	public final static int APPLICATION_FORM_URLENCODED_TYPE = 6;
	public final static String MULTIPART_FORM_DATA = "multipart/form-data";
	public final static int MULTIPART_FORM_DATA_TYPE = 7;
	public final static String APPLICATION_OCTET_STREAM = "application/octet-stream";
	public final static int APPLICATION_OCTET_STREAM_TYPE = 8;
	public final static String TEXT_PLAIN = "text/plain";
	public final static int TEXT_PLAIN_TYPE = 9;
	public final static String TEXT_XML = "text/xml";
	public final static int TEXT_XML_TYPE = 10;
	public final static String TEXT_HTML = "text/html";
	public final static int TEXT_HTML_TYPE = 11;



	public Message() {
		// This method is only to init class for de-serialization
		this.uuid = null;
		this.mimeType = -1;
		this.payload = null;
		this.received_at = -1;
		this.stored = false;
	}

	public Message(int mimeType)
	{
		this.uuid = UUID.randomUUID().toString();
		this.mimeType = mimeType;
		this.payload = new byte[0];
		this.received_at = DateTime.now().getMillis();
		this.stored = true;
	}

	public Message(int mimeType, byte[] payload)
	{
		this.uuid = UUID.randomUUID().toString();
		this.mimeType = mimeType;
		this.payload = payload;
		this.received_at = DateTime.now().getMillis();
		this.stored = false;
	}

	public String getUuid()
	{
		return uuid;
	}

	public int getMimeType()
	{
		return mimeType;
	}

	public byte[] getPayload()
	{
		return payload;
	}

	public DateTime getReceived_at()
	{
		return new DateTime(received_at);
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

		if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) return false;

		return true;
	}

	public int hashCode()
	{
		return  uuid != null ? uuid.hashCode() : 0;
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
