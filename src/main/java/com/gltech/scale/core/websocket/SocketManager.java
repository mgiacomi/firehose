package com.gltech.scale.core.websocket;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;

public interface SocketManager
{
	AggregatorSocket getAggregatorSocket(ServiceMetaData serviceMetaData);
}
