package com.gltech.scale.core.storage.bytearray;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.gltech.scale.core.storage.BucketMetaData;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class StoragePayload
{
	private static final JsonFactory jsonFactory = new JsonFactory();

	private String customer;
	private String bucket;
	private DateTime created_at;
	private byte[] payload;
	private String id;

	private transient BucketMetaData bucketMetaData;
	private transient List<String> previousVersions = Collections.emptyList();

	public StoragePayload(String id, String customer, String bucket, byte[] payload)
	{
		this.payload = payload;
		created_at = DateTime.now();
		this.customer = customer;
		this.bucket = bucket;
		this.id = id;
	}

	private StoragePayload()
	{
	}

	public List<String> getPreviousVersions()
	{
		return previousVersions;
	}

	public void setPreviousVersions(List<String> previousVersions)
	{
		if (previousVersions == null)
		{
			return;
		}
		this.previousVersions = previousVersions;
	}

	public String getCustomer()
	{
		return customer;
	}

	public String getBucket()
	{
		return bucket;
	}

	public DateTime getCreated_at()
	{
		return created_at;
	}

	public void setCreated_at(DateTime created_at)
	{
		this.created_at = created_at;
	}

	public byte[] getPayload()
	{
		return payload;
	}

	public String getId()
	{
		return id;
	}

	public String getVersion()
	{
		return DigestUtils.md5Hex(payload);
	}

	public BucketMetaData getBucketMetaData()
	{
		return bucketMetaData;
	}

	public void setBucketMetaData(BucketMetaData bucketMetaData)
	{
		this.bucketMetaData = bucketMetaData;
	}

	public byte[] convert() throws IOException
	{
		//todo - gfm - 10/9/12 - test with NIO

		try (ByteArrayOutputStream baos = new ByteArrayOutputStream())
		{
			JsonGenerator generator = jsonFactory.createJsonGenerator(baos);
			generator.writeStartObject();
			generator.writeStringField("id", getId());
			generator.writeStringField("customer", getCustomer());
			generator.writeStringField("bucket", getBucket());
			generator.writeBinaryField("payload", getPayload());
			generator.writeNumberField("created", getCreated_at().getMillis());
			generator.writeEndObject();
			generator.close();
			return baos.toByteArray();
		}
	}

	public static StoragePayload convert(byte[] bytes) throws IOException
	{
		if (bytes == null || bytes.length == 0)
		{
			return null;
		}

		try (JsonParser parser = jsonFactory.createJsonParser(bytes))
		{
			StoragePayload storagePayload = new StoragePayload();
			while (parser.nextValue() != null)
			{
				while (parser.nextToken() != null)
				{
					String fieldName = parser.getCurrentName();
					parser.nextToken();
					if (fieldName != null)
					{
						switch (fieldName)
						{
							case "id":
								storagePayload.id = parser.getText();
								break;
							case "customer":
								storagePayload.customer = parser.getText();
								break;
							case "bucket":
								storagePayload.bucket = parser.getText();
								break;
							case "payload":
								storagePayload.payload = parser.getBinaryValue();
								break;
							case "created":
								storagePayload.setCreated_at(new DateTime(parser.getLongValue()));
								break;
							default:
								break;
						}
					}
				}

			}
			return storagePayload;
		}
	}

	public String toString()
	{
		return "StoragePayload{" +
				"customer='" + customer + '\'' +
				", bucket='" + bucket + '\'' +
				", id='" + id + '\'' +
				", created_at=" + created_at +
				'}';
	}
}
