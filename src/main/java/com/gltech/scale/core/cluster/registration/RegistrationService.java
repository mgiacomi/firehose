package com.gltech.scale.core.cluster.registration;

import java.util.List;

public interface RegistrationService
{
	void registerAsInboundService();

	void unRegisterAsInboundService();

	ServiceMetaData getLocalStorageWriterMetaData();

	void registerAsStorageWriter();

	void unRegisterAsStorageWriter();

	void registerAsAggregator();

	void unRegisterAsAggregator();

	List<ServiceMetaData> getRegisteredAggregators();

	ServiceMetaData getLocalAggregatorMetaData();

	ServiceMetaData getAggregatorMetaDataById(String id);

	void shutdown();
}
