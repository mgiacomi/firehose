package com.gltech.scale.core.websocket;

import com.gltech.scale.core.aggregator.clientserver.AggregatorClientSocket;
import com.gltech.scale.core.cluster.registration.ServiceMetaData;

public interface SocketManager
{
	AggregatorClientSocket getAggregatorSocket(ServiceMetaData serviceMetaData);
}
