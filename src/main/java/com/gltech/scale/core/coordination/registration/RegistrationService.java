package com.gltech.scale.core.coordination.registration;

import java.util.List;

public interface RegistrationService
{
	void registerAsEventService();

	void unRegisterAsEventService();

	ServiceMetaData getLocalCollectorManagerMetaData();

	void registerAsCollectorManager();

	void unRegisterAsCollectorManager();

	void registerAsRopeManager();

	void unRegisterAsRopeManager();

	ServiceMetaData getLocalRopeManagerMetaData();

	ServiceMetaData getRopeManagerMetaDataById(String id);

	void registerAsStorageService();

	void unRegisterAsStorageService();

	ServiceMetaData getStorageServiceRandom();

	ServiceMetaData getStorageServiceRoundRobin();

	ServiceMetaData getStorageServiceSticky();

	List<ServiceMetaData> getRegisteredRopeManagers();

	void shutdown();
}
