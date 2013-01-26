package com.gltech.scale.core.rope;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.gltech.scale.core.coordination.registration.ServiceMetaData;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static junit.framework.Assert.*;

public class PrimaryBackupSetTest
{
	@Test
	public void testToJson() throws Exception
	{
		ServiceMetaData storageService1 = new ServiceMetaData();
		storageService1.setListenAddress("0.0.0.0");
		storageService1.setListenPort(0);
		storageService1.setWorkerId(UUID.randomUUID());

		ServiceMetaData storageService2 = new ServiceMetaData();
		storageService2.setListenAddress("1.1.1.1");
		storageService2.setListenPort(1);
		storageService2.setWorkerId(UUID.randomUUID());

		PrimaryBackupSet primaryBackupSet = new PrimaryBackupSet(storageService1, storageService2);

		ObjectMapper mapper = new ObjectMapper();

		String json = mapper.writeValueAsString(primaryBackupSet);
		PrimaryBackupSet primaryBackupSet1 = mapper.readValue(json, PrimaryBackupSet.class);

		assertEquals(primaryBackupSet.getPrimary(), primaryBackupSet1.getPrimary());
		assertEquals(primaryBackupSet.getBackup(), primaryBackupSet1.getBackup());
	}

	@Test
	public void testListToJson() throws Exception
	{

		ServiceMetaData storageService1 = new ServiceMetaData();
		storageService1.setListenAddress("0.0.0.0");
		storageService1.setListenPort(0);
		storageService1.setWorkerId(UUID.randomUUID());

		ServiceMetaData storageService2 = new ServiceMetaData();
		storageService2.setListenAddress("1.1.1.1");
		storageService2.setListenPort(1);
		storageService2.setWorkerId(UUID.randomUUID());

		ServiceMetaData storageService3 = new ServiceMetaData();
		storageService3.setListenAddress("2.2.2.2");
		storageService3.setListenPort(2);
		storageService3.setWorkerId(UUID.randomUUID());

		ServiceMetaData storageService4 = new ServiceMetaData();
		storageService4.setListenAddress("3.3.3.3");
		storageService4.setListenPort(3);
		storageService4.setWorkerId(UUID.randomUUID());


		PrimaryBackupSet primaryBackupSet1 = new PrimaryBackupSet(storageService1, storageService2);
		PrimaryBackupSet primaryBackupSet2 = new PrimaryBackupSet(storageService3, storageService4);

		List<PrimaryBackupSet> primaryBackupSets = new ArrayList<>();
		primaryBackupSets.add(primaryBackupSet1);
		primaryBackupSets.add(primaryBackupSet2);

		RopeManagersByPeriod ropeManagersByPeriod = new RopeManagersByPeriod(new DateTime(), primaryBackupSets);

		ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule(new JodaModule());

		String json = mapper.writeValueAsString(ropeManagersByPeriod);
		RopeManagersByPeriod ropeManagersByPeriod1 = mapper.readValue(json, RopeManagersByPeriod.class);

		assertEquals(ropeManagersByPeriod.getPeriod().getMillis(), ropeManagersByPeriod1.getPeriod().getMillis());

		PrimaryBackupSet primaryBackupSet = ropeManagersByPeriod.nextPrimaryBackupSet();
		assertEquals(primaryBackupSet.getPrimary(), primaryBackupSet1.getPrimary());
		assertEquals(primaryBackupSet.getBackup(), primaryBackupSet1.getBackup());

		primaryBackupSet = ropeManagersByPeriod.nextPrimaryBackupSet();
		assertEquals(primaryBackupSet.getPrimary(), primaryBackupSet2.getPrimary());
		assertEquals(primaryBackupSet.getBackup(), primaryBackupSet2.getBackup());
	}
}
