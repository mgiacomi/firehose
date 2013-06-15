package com.gltech.scale.core.cluster.registration;

import org.joda.time.DateTime;

import java.util.List;
import java.util.Set;

public interface RegistrationService
{
	Set<String> getRoles();

	DateTime getLocalServerRegistrationTime();

	ServiceMetaData getLocalServerMetaData();

	void registerAsServer();

	void unRegisterAsServer();

	List<ServiceMetaData> getRegisteredServers();

	void registerAsInboundService();

	void unRegisterAsInboundService();

	void registerAsOutboundService();

	void unRegisterAsOutboundService();

	void registerAsStorageWriter();

	void unRegisterAsStorageWriter();

	void registerAsAggregator();

	void unRegisterAsAggregator();

	List<ServiceMetaData> getRegisteredAggregators();

	ServiceMetaData getAggregatorMetaDataById(String id);

	void shutdown();
}
