package com.gltech.scale.core.cluster.registration;

import java.util.List;

public interface RegistrationService
{
	void registerAsEventService();

	void unRegisterAsEventService();

	ServiceMetaData getLocalCollectorManagerMetaData();

	void registerAsCollectorManager();

	void unRegisterAsCollectorManager();

	void registerAsAggregator();

	void unRegisterAsAggregator();

	ServiceMetaData getLocalAggregatorMetaData();

	ServiceMetaData getAggregatorMetaDataById(String id);

	void registerAsStorageService();

	void unRegisterAsStorageService();

	ServiceMetaData getStorageServiceRandom();

	ServiceMetaData getStorageServiceRoundRobin();

	ServiceMetaData getStorageServiceSticky();

	List<ServiceMetaData> getRegisteredAggregators();

	void shutdown();
}
