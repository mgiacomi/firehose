package com.gltech.scale.core.storage.bytearray;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.gltech.scale.core.model.ChannelMetaData;
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

	public void putBucket(ChannelMetaData channelMetaData)
	{
		byteArrayStorage.putBucket(channelMetaData);
	}

	public ChannelMetaData getBucket(String channelName)
	{
		return byteArrayStorage.getBucket(channelName);
	}

	public void putPayload(String channelName, String id, InputStream inputStream, Map<String, List<String>> headers)
	{
//		try
//		{
			//todo - gfm - 10/2/12 - handle if-none-match - http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.26
//			StoragePayload storagePayload = new StoragePayload(id, channelName, IOUtils.toByteArray(inputStream));
StoragePayload storagePayload = null;
			storagePayload.setPreviousVersions(headers.get(HttpHeaders.IF_MATCH));
			byteArrayStorage.putPayload(storagePayload);
//		}
//		catch (IOException e)
//		{
//			throw Throwables.propagate(e);
//		}
	}

	public void getPayload(String channelName, String id, OutputStream outputStream)
	{
		StoragePayload payload = byteArrayStorage.getPayload(channelName, id);

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
