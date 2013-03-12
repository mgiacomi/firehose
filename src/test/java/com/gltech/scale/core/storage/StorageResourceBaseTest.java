package com.gltech.scale.core.storage;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.server.EmbeddedServer;
import com.gltech.scale.core.storage.bytearray.ByteArrayStorage;
import com.gltech.scale.core.util.ClientCreator;
import com.gltech.scale.core.util.Props;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import static junit.framework.Assert.*;

/**
 * Use this class to inherit Storage Resource Tests with different backends
 */
public abstract class StorageResourceBaseTest
{
	Client client;
	private StorageServiceRestClient restClient;

	@Before
	public void setUp() throws Exception
	{
		client = ClientCreator.createCached();
		restClient = new StorageServiceRestClient();
		Props.getProps().set("zookeeper.throw_unregister_exception", false);
	}

	@Test
	public void testBucket()
	{
		String customer = "testCustomer";
		String bucket = "testBucket";
		BucketMetaData eventSetBucket = BucketMetaDataTest.createEventSetBucket(customer, bucket);
		WebResource resource = client.resource("http://localhost:9090/storage/" + customer + "/" + bucket);
		ClientResponse putResponse = resource.put(ClientResponse.class, eventSetBucket.toJson().toString());
		assertEquals(201, putResponse.getStatus());
		assertEquals(resource.getURI().toString(), putResponse.getLocation().toString());
		ClientResponse getResponse = resource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
		assertEquals(200, getResponse.getStatus());
		assertEquals(eventSetBucket.toJson().toString(), getResponse.getEntity(String.class));
	}

	private void simpleCreate(String customer, String bucket)
	{
		ServiceMetaData storageService = new ServiceMetaData();
		storageService.setListenAddress(Props.getProps().get("event_service.rest_host", "localhost"));
		storageService.setListenPort(Props.getProps().get("event_service.rest_port", 9090));

		BucketMetaData eventSetBucket = BucketMetaDataTest.createEventSetBucket(customer, bucket);
		restClient.putBucketMetaData(storageService, eventSetBucket);
		BucketMetaData bucketMetaData = restClient.getBucketMetaData(storageService, customer, bucket);
		assertNotNull(bucketMetaData);
	}

	@Test
	public void testBucketNotFound()
	{
		String customer = "testCustomer";
		String bucket = "testBucketNotFound";
		WebResource resource = client.resource("http://localhost:9090/storage/" + customer + "/" + bucket);
		ClientResponse getResponse = resource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
		assertEquals(404, getResponse.getStatus());

	}

	@Test
	public void testMalformedBucket()
	{
		String customer = "testCustomer";
		String bucket = "testMalformedBucket";

		WebResource resource = client.resource("http://localhost:9090/storage/" + customer + "/" + bucket);
		ClientResponse putResponse = resource.put(ClientResponse.class, "{}");
		assertEquals(400, putResponse.getStatus());
	}

	@Test
	public void testExistingBucket()
	{
		String customer = "testCustomer";
		String bucket = "testExistingBucket";
		BucketMetaData eventSetBucket = BucketMetaDataTest.createEventSetBucket(customer, bucket);
		WebResource resource = client.resource("http://localhost:9090/storage/" + customer + "/" + bucket);
		ClientResponse putResponse = resource.put(ClientResponse.class, eventSetBucket.toJson().toString());
		assertEquals(201, putResponse.getStatus());
		putResponse = resource.put(ClientResponse.class, eventSetBucket.toJson().toString());
		assertEquals(403, putResponse.getStatus());
		assertTrue(putResponse.getEntity(String.class).contains(bucket));
	}

	@Test
	public void testEncodingNames()
	{
		ByteArrayStorage byteArrayStorage = EmbeddedServer.getInjector().getInstance(ByteArrayStorage.class);
		simpleCreate("test Customer", "test Bucket");
		assertNotNull(byteArrayStorage.getBucket("test+Customer", "test+Bucket"));
		simpleCreate("$%+,/:;=?@", "$%+,/:;=?@");
		assertNotNull(byteArrayStorage.getBucket("$%+,/:;=?@", "$%+,/:;=?@"));
	}

	@Test(expected = BucketMetaDataException.class)
	public void testCustomerName()
	{
		simpleCreate("test|Customer", "testBucket");
	}

	@Test(expected = BucketMetaDataException.class)
	public void testBucketName()
	{
		simpleCreate("testCustomer", "test|Bucket");
	}

	@Test
	public void testPayloads()
	{
		WebResource resource = createPayloadsBucket();
		String payload = "This is a payload.";
		WebResource payloadResource = resource.path("/123");
		ClientResponse payloadResponse = payloadResource.put(ClientResponse.class, payload);
		assertEquals(201, payloadResponse.getStatus());
		payloadResponse = payloadResource.get(ClientResponse.class);
		assertEquals(200, payloadResponse.getStatus());
		assertEquals(payload, payloadResponse.getEntity(String.class));

	}

	private WebResource createPayloadsBucket()
	{
		String customer = "testCustomer";
		String bucket = "payloadBucket";
		BucketMetaData eventSetBucket = BucketMetaDataTest.createEventSetBucket(customer, bucket);
		WebResource resource = client.resource("http://localhost:9090/storage/" + customer + "/" + bucket);
		resource.put(ClientResponse.class, eventSetBucket.toJson().toString());
		return resource;
	}

	@Test
	public void testPayloadMissing()
	{
		WebResource resource = createPayloadsBucket();
		ClientResponse response = resource.path("ABCD").get(ClientResponse.class);
		assertEquals(404, response.getStatus());
	}

	@Test
	public void testPayloadMediaType()
	{
		String customer = "testCustomer";
		String bucket = "payloadBucket";
		BucketMetaData eventSetBucket = BucketMetaDataTest.createEventSetBucket(customer, bucket);
		WebResource resource = client.resource("http://localhost:9090/storage/" + customer + "/" + bucket);
		resource.put(ClientResponse.class, eventSetBucket.toJson().toString());
		ClientResponse response = resource.get(ClientResponse.class);
		assertEquals(200, response.getStatus());
		assertEquals(MediaType.APPLICATION_JSON, response.getType().toString());

	}

	@Test
	public void testETags()
	{
		WebResource resource = createPayloadsBucket();
		String payload = "the first payload";
		WebResource payloadResource = resource.path("/etagger");
		payloadResource.put(ClientResponse.class, payload);
		ClientResponse response = payloadResource.get(ClientResponse.class);
		assertEquals(200, response.getStatus());
		assertEquals(payload, response.getEntity(String.class));
		assertEquals(DigestUtils.md5Hex(payload), response.getHeaders().getFirst(HttpHeaders.ETAG));

		payload = "the second payload";
		payloadResource.put(ClientResponse.class, payload);
		response = payloadResource.get(ClientResponse.class);
		assertEquals(200, response.getStatus());
		assertEquals(payload, response.getEntity(String.class));
		assertEquals(DigestUtils.md5Hex(payload), response.getHeaders().getFirst(HttpHeaders.ETAG));
	}

	@Test
	public void testEtagIfMatch()
	{
		WebResource resource = createPayloadsBucket();
		String payload = "the first payload";
		WebResource payloadResource = resource.path("/testEtagIfMatch");
		ClientResponse response = payloadResource.put(ClientResponse.class, payload);
		assertEquals(201, response.getStatus());
		response = payloadResource.header(HttpHeaders.IF_MATCH, "blah")
				.put(ClientResponse.class, "another");
		assertEquals(412, response.getStatus());
		response = payloadResource.get(ClientResponse.class);
		assertEquals(200, response.getStatus());
		String etag = response.getHeaders().getFirst(HttpHeaders.ETAG);

		response = payloadResource.header(HttpHeaders.ETAG, etag)
				.put(ClientResponse.class, "another");
		assertEquals(201, response.getStatus());
	}

	@Test
	public void testEtagBeforeCreation()
	{
		WebResource resource = createPayloadsBucket();
		String payload = "the first payload";
		WebResource payloadResource = resource.path("/testEtagBeforeCreation");
		ClientResponse response = payloadResource
				.header(HttpHeaders.IF_MATCH, "blah")
				.put(ClientResponse.class, payload);
		assertEquals(412, response.getStatus());
	}

	@Test
	public void testEtagIfMatchStar()
	{
		WebResource resource = createPayloadsBucket();
		String payload = "the first payload";
		WebResource payloadResource = resource.path("/testEtagIfMatchStar");
		ClientResponse response = payloadResource
				.header(HttpHeaders.IF_MATCH, "*")
				.put(ClientResponse.class, payload);
		assertEquals(412, response.getStatus());

		response = payloadResource.put(ClientResponse.class, payload);
		assertEquals(201, response.getStatus());

		response = payloadResource
				.header(HttpHeaders.IF_MATCH, "*")
				.put(ClientResponse.class, "whatever");

		assertEquals(201, response.getStatus());
	}
}
