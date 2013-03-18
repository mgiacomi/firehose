package com.gltech.scale.core.model;

import com.dyuproject.protostuff.Tag;
import org.joda.time.DateTime;

import javax.ws.rs.core.MediaType;
import java.util.UUID;

public class Message
{
	@Tag(1)
	private final String uuid;
	@Tag(2)
	private final int mimeTypeId;
	@Tag(3)
	private final byte[] payload;
	@Tag(4)
	private final long received_at;
	@Tag(5)
	private final boolean stored;

	public Message()
	{
		// This method is only to init class for de-serialization
		this.uuid = null;
		this.mimeTypeId = -1;
		this.payload = null;
		this.received_at = -1;
		this.stored = false;
	}

	public Message(MediaType mediaType)
	{
		this.uuid = UUID.randomUUID().toString();
		this.mimeTypeId = getIdForMediaType(mediaType);
		this.payload = new byte[0];
		this.received_at = DateTime.now().getMillis();
		this.stored = true;
	}

	public Message(MediaType mediaType, byte[] payload)
	{
		this.uuid = UUID.randomUUID().toString();
		this.mimeTypeId = getIdForMediaType(mediaType);
		this.payload = payload;
		this.received_at = DateTime.now().getMillis();
		this.stored = false;
	}

	public String getUuid()
	{
		return uuid;
	}

	public int getMimeTypeId()
	{
		return mimeTypeId;
	}

	public String getMimeType()
	{
		if(mimeTypeId == 1) {
			return MediaType.APPLICATION_XML;
		}
		if(mimeTypeId == 2) {
			return MediaType.APPLICATION_ATOM_XML;
		}
		if(mimeTypeId == 3) {
			return MediaType.APPLICATION_XHTML_XML;
		}
		if(mimeTypeId == 4) {
			return MediaType.APPLICATION_SVG_XML;
		}
		if(mimeTypeId == 5) {
			return MediaType.APPLICATION_JSON;
		}
		if(mimeTypeId == 6) {
			return MediaType.APPLICATION_FORM_URLENCODED;
		}
		if(mimeTypeId == 7) {
			return MediaType.MULTIPART_FORM_DATA;
		}
		if(mimeTypeId == 8) {
			return MediaType.APPLICATION_OCTET_STREAM;
		}
		if(mimeTypeId == 9) {
			return MediaType.TEXT_PLAIN;
		}
		if(mimeTypeId == 10) {
			return MediaType.TEXT_XML;
		}
		if(mimeTypeId == 11) {
			return MediaType.TEXT_HTML;
		}

		throw new IllegalStateException("MediaType not found.");
	}

	public MediaType getMediaType()
	{
		if(mimeTypeId == 1) {
			return MediaType.APPLICATION_XML_TYPE;
		}
		if(mimeTypeId == 2) {
			return MediaType.APPLICATION_ATOM_XML_TYPE;
		}
		if(mimeTypeId == 3) {
			return MediaType.APPLICATION_XHTML_XML_TYPE;
		}
		if(mimeTypeId == 4) {
			return MediaType.APPLICATION_SVG_XML_TYPE;
		}
		if(mimeTypeId == 5) {
			return MediaType.APPLICATION_JSON_TYPE;
		}
		if(mimeTypeId == 6) {
			return MediaType.APPLICATION_FORM_URLENCODED_TYPE;
		}
		if(mimeTypeId == 7) {
			return MediaType.MULTIPART_FORM_DATA_TYPE;
		}
		if(mimeTypeId == 8) {
			return MediaType.APPLICATION_OCTET_STREAM_TYPE;
		}
		if(mimeTypeId == 9) {
			return MediaType.TEXT_PLAIN_TYPE;
		}
		if(mimeTypeId == 10) {
			return MediaType.TEXT_XML_TYPE;
		}
		if(mimeTypeId == 11) {
			return MediaType.TEXT_HTML_TYPE;
		}

		throw new IllegalStateException("MediaType not found.");
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
		return uuid != null ? uuid.hashCode() : 0;
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

	private int getIdForMediaType(MediaType mediaType)
	{
		if(mediaType.equals(MediaType.APPLICATION_XML_TYPE)) {
			return 1;
		}
		if(mediaType.equals(MediaType.APPLICATION_ATOM_XML_TYPE)) {
			return 2;
		}
		if(mediaType.equals(MediaType.APPLICATION_XHTML_XML_TYPE)) {
			return 3;
		}
		if(mediaType.equals(MediaType.APPLICATION_SVG_XML_TYPE)) {
			return 4;
		}
		if(mediaType.equals(MediaType.APPLICATION_JSON_TYPE)) {
			return 5;
		}
		if(mediaType.equals(MediaType.APPLICATION_FORM_URLENCODED_TYPE)) {
			return 6;
		}
		if(mediaType.equals(MediaType.MULTIPART_FORM_DATA_TYPE)) {
			return 7;
		}
		if(mediaType.equals(MediaType.APPLICATION_OCTET_STREAM_TYPE)) {
			return 8;
		}
		if(mediaType.equals(MediaType.TEXT_PLAIN_TYPE)) {
			return 9;
		}
		if(mediaType.equals(MediaType.TEXT_XML_TYPE)) {
			return 10;
		}
		if(mediaType.equals(MediaType.TEXT_HTML_TYPE)) {
			return 11;
		}

		throw new IllegalStateException("No MediaType match found: "+ mediaType.toString());
	}

	public String toString()
	{
		return "Message{" +
				"uuid='" + uuid + '\'' +
				", mimeTypeId=" + mimeTypeId +
				", payloadsize=" + payload.length +
				", received_at=" + received_at +
				", stored=" + stored +
				'}';
	}
}
