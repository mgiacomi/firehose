package com.gltech.scale.core.storage.bytearray;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.gltech.scale.core.storage.BucketMetaData;
import com.gltech.scale.core.storage.Storage;
import org.apache.commons.io.IOUtils;

import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class ByteArrayStorageAdapter implements Storage
{
	private ByteArrayStorage byteArrayStorage;

	@Inject
	public ByteArrayStorageAdapter(ByteArrayStorage byteArrayStorage)
	{
		this.byteArrayStorage = byteArrayStorage;
	}

	public void putBucket(BucketMetaData bucketMetaData)
	{
		byteArrayStorage.putBucket(bucketMetaData);
	}

	public BucketMetaData getBucket(String customer, String bucket)
	{
		return byteArrayStorage.getBucket(customer, bucket);
	}

	public void putPayload(String customer, String bucket, String id, InputStream inputStream, Map<String, List<String>> headers)
	{
		try
		{
			//todo - gfm - 10/2/12 - handle if-none-match - http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.26
			StoragePayload storagePayload = new StoragePayload(id, customer, bucket, IOUtils.toByteArray(inputStream));
			storagePayload.setPreviousVersions(headers.get(HttpHeaders.IF_MATCH));
			byteArrayStorage.putPayload(storagePayload);
		}
		catch (IOException e)
		{
			throw Throwables.propagate(e);
		}
	}

	public void getPayload(String customer, String bucket, String id, OutputStream outputStream)
	{
		StoragePayload payload = byteArrayStorage.getPayload(customer, bucket, id);

		if (payload != null)
		{
			/* todo: mlg 1/4/13 - need to add ETAG support back.
			Response.ResponseBuilder builder = Response.ok(payload.getPayload());
			builder.header(HttpHeaders.ETAG, payload.getVersion());
			return builder.build();
			*/

			try
			{
				outputStream.write(payload.getPayload());
			}
			catch (IOException e)
			{
				throw Throwables.propagate(e);
			}
		}
	}
}