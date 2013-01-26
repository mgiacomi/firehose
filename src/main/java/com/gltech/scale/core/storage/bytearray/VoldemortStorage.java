package com.gltech.scale.core.storage.bytearray;

import com.gltech.scale.core.storage.BucketMetaData;
import com.gltech.scale.core.storage.DuplicateBucketException;
import com.gltech.scale.core.voldemort.VoldemortUtil;
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
	private static final Logger logger = LoggerFactory.getLogger("com.lokiscale.storage.VoldemortStorage");

	private StoreClient<Map<String, String>, String> bucketClient;
	private StoreClient<Map<String, String>, byte[]> payloadClient;

	public VoldemortStorage()
	{
		this.bucketClient = VoldemortUtil.createFactory().getStoreClient("BucketStorage");
		this.payloadClient = VoldemortUtil.createFactory().getStoreClient("ShortTermPayloadStorage");
	}

	public VoldemortStorage(String storeName)
	{
		this.bucketClient = VoldemortUtil.createFactory().getStoreClient("BucketStorage");
		this.payloadClient = VoldemortUtil.createFactory().getStoreClient(storeName);
	}

	public void putBucket(final BucketMetaData bucketMetaData)
	{
		final Map<String, String> keyMap = createBucketKeyMap(bucketMetaData.getCustomer(),
				bucketMetaData.getBucket());
		bucketClient.applyUpdate(new UpdateAction<Map<String, String>, String>()
		{
			public void update(StoreClient<Map<String, String>, String> client)
			{
				Versioned<String> versioned = client.get(keyMap);
				if (versioned == null)
				{
					versioned = new Versioned<>(bucketMetaData.toJson().toString());
					client.put(keyMap, versioned);
					return;
				}
				throw new DuplicateBucketException("Bucket already exists "
						+ bucketMetaData.getCustomer() + " " + bucketMetaData.getBucket());

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

	public BucketMetaData getBucket(String customer, String bucket)
	{
		Map<String, String> keyMap = createBucketKeyMap(customer, bucket);
		String json = bucketClient.getValue(keyMap);
		if (json == null)
		{
			return null;
		}
		return new BucketMetaData(customer, bucket, json);
	}

	private Map<String, String> createPayloadKeyMap(String customer, String bucket, String id)
	{
		Map<String, String> bucketKeyMap = createBucketKeyMap(customer, bucket);
		bucketKeyMap.put("id", id);
		return bucketKeyMap;
	}

	public StoragePayload internalGetPayload(BucketMetaData bucketMetaData, String id)
	{
		try
		{
			Map<String, String> payloadKeyMap = createPayloadKeyMap(bucketMetaData.getCustomer(), bucketMetaData.getBucket(), id);
			Versioned<byte[]> versioned = payloadClient.get(payloadKeyMap);
			if (versioned == null)
			{
				return null;
			}
			StoragePayload payload = StoragePayload.convert(versioned.getValue());
			if (payload != null)
			{
				payload.setBucketMetaData(bucketMetaData);
			}
			return payload;
		}
		catch (IOException e)
		{
			logger.warn("unable to parse payload {} {} {}", bucketMetaData, id, e);
			return null;
		}
	}

	public StoragePayload getPayload(String customer, String bucket, String id)
	{
		return internalGetPayload(getBucket(customer, bucket), id);
	}

	public void internalPutPayload(BucketMetaData bucketMetaData, StoragePayload storagePayload)
	{
		try
		{
			final Map<String, String> payloadKeyMap = createPayloadKeyMap(bucketMetaData.getCustomer(), bucketMetaData.getBucket(), storagePayload.getId());
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
