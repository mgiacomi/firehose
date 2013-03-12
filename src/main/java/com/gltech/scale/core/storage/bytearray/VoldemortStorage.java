package com.gltech.scale.core.storage.bytearray;

import com.gltech.scale.core.model.ChannelMetaData;
import com.gltech.scale.core.storage.DuplicateBucketException;
import com.gltech.scale.util.VoldemortClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import voldemort.client.StoreClient;
import voldemort.client.UpdateAction;
import voldemort.versioning.Versioned;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class VoldemortStorage implements InternalStorage
{
	private static final Logger logger = LoggerFactory.getLogger(VoldemortStorage.class);

	private StoreClient<Map<String, String>, String> bucketClient;
	private StoreClient<Map<String, String>, byte[]> payloadClient;

	public VoldemortStorage()
	{
		this.bucketClient = VoldemortClient.createFactory().getStoreClient("BucketStorage");
		this.payloadClient = VoldemortClient.createFactory().getStoreClient("ShortTermPayloadStorage");
	}

	public VoldemortStorage(String storeName)
	{
		this.bucketClient = VoldemortClient.createFactory().getStoreClient("BucketStorage");
		this.payloadClient = VoldemortClient.createFactory().getStoreClient(storeName);
	}

	public void putBucket(final ChannelMetaData channelMetaData)
	{
		final Map<String, String> keyMap = createBucketKeyMap(channelMetaData.getCustomer(),
				channelMetaData.getBucket());
		bucketClient.applyUpdate(new UpdateAction<Map<String, String>, String>()
		{
			public void update(StoreClient<Map<String, String>, String> client)
			{
				Versioned<String> versioned = client.get(keyMap);
				if (versioned == null)
				{
					versioned = new Versioned<>(channelMetaData.toJson().toString());
					client.put(keyMap, versioned);
					return;
				}
				throw new DuplicateBucketException("Bucket already exists "
						+ channelMetaData.getCustomer() + " " + channelMetaData.getBucket());

			}
		});
	}

	private Map<String, String> createBucketKeyMap(String customer, String bucket)
	{
		final Map<String, String> keyMap = new HashMap<>();
		keyMap.put("customer", customer);
		keyMap.put("bucket", bucket);
		return keyMap;
	}

	public ChannelMetaData getBucket(String customer, String bucket)
	{
		Map<String, String> keyMap = createBucketKeyMap(customer, bucket);
		String json = bucketClient.getValue(keyMap);
		if (json == null)
		{
			return null;
		}
		return new ChannelMetaData(customer, bucket, json);
	}

	private Map<String, String> createPayloadKeyMap(String customer, String bucket, String id)
	{
		Map<String, String> bucketKeyMap = createBucketKeyMap(customer, bucket);
		bucketKeyMap.put("id", id);
		return bucketKeyMap;
	}

	public StoragePayload internalGetPayload(ChannelMetaData channelMetaData, String id)
	{
		try
		{
			Map<String, String> payloadKeyMap = createPayloadKeyMap(channelMetaData.getCustomer(), channelMetaData.getBucket(), id);
			Versioned<byte[]> versioned = payloadClient.get(payloadKeyMap);
			if (versioned == null)
			{
				return null;
			}
			StoragePayload payload = StoragePayload.convert(versioned.getValue());
			if (payload != null)
			{
				payload.setChannelMetaData(channelMetaData);
			}
			return payload;
		}
		catch (IOException e)
		{
			logger.warn("unable to parse payload {} {} {}", channelMetaData, id, e);
			return null;
		}
	}

	public StoragePayload getPayload(String customer, String bucket, String id)
	{
		return internalGetPayload(getBucket(customer, bucket), id);
	}

	public void internalPutPayload(ChannelMetaData channelMetaData, StoragePayload storagePayload)
	{
		try
		{
			final Map<String, String> payloadKeyMap = createPayloadKeyMap(channelMetaData.getCustomer(), channelMetaData.getBucket(), storagePayload.getId());
			final byte[] bytes = storagePayload.convert();
			logger.debug("putting {} bytes in payload {}", bytes.length, payloadKeyMap);
			payloadClient.applyUpdate(new UpdateAction<Map<String, String>, byte[]>()
			{
				public void update(StoreClient<Map<String, String>, byte[]> storeClient)
				{
					Versioned<byte[]> versioned = storeClient.get(payloadKeyMap);
					if (versioned == null)
					{
						versioned = new Versioned<>(bytes);
					}
					else
					{
						versioned.setObject(bytes);
					}
					storeClient.put(payloadKeyMap, versioned);
				}
			});
		}
		catch (IOException e)
		{
			logger.warn("unable to put payload {}", storagePayload, e);
		}
	}

	public void putPayload(StoragePayload storagePayload)
	{
		internalPutPayload(getBucket(storagePayload.getCustomer(), storagePayload.getBucket()), storagePayload);
	}
}
