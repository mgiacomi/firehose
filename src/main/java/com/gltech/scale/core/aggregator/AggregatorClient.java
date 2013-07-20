package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.cluster.registration.ServiceMetaData;
import com.gltech.scale.core.model.Defaults;
import com.gltech.scale.core.model.Message;
import com.gltech.scale.core.model.ModelIO;
import com.gltech.scale.core.websocket.*;
import com.gltech.scale.util.Props;
import com.google.inject.Inject;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public interface AggregatorClient
{
	void sendMessage(ServiceMetaData primary, String channelName, DateTime nearestPeriodCeiling, Message message);

	void sendMessage(ServiceMetaData primary, ServiceMetaData backup, String channelName, DateTime nearestPeriodCeiling, Message message);
}
