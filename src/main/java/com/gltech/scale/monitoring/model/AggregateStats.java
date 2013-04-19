package com.gltech.scale.monitoring.model;

import com.dyuproject.protostuff.Tag;

public class AggregateStats
{
	@Tag(1)
	private int inboundLoad = 0;
	@Tag(2)
	private int inboundAvgMsgSize = 0;
	@Tag(3)
	private int inboundMsgPerSec = 0;
	@Tag(4)
	private int aggregatorMsgInQue = 0;
	@Tag(5)
	private int aggregatorQueSize = 0;
	@Tag(6)
	private int aggregatorQueAge = 0;
	@Tag(7)
	private int storageWriterMsgPerSec = 0;
	@Tag(8)
	private int storageWriterBytesPerSec = 0;
	@Tag(9)
	private int storageWriterBatchesBeingWritten = 0;
	@Tag(10)
	private int outboundMsgPerSec = 0;
	@Tag(11)
	private int outboundAvgMsgSize = 0;
	@Tag(12)
	private int outboundActiveQueries = 0;

	public int getInboundLoad()
	{
		return inboundLoad;
	}

	public void setInboundLoad(int inboundLoad)
	{
		this.inboundLoad = inboundLoad;
	}

	public int getInboundAvgMsgSize()
	{
		return inboundAvgMsgSize;
	}

	public void setInboundAvgMsgSize(int inboundAvgMsgSize)
	{
		this.inboundAvgMsgSize = inboundAvgMsgSize;
	}

	public int getInboundMsgPerSec()
	{
		return inboundMsgPerSec;
	}

	public void setInboundMsgPerSec(int inboundMsgPerSec)
	{
		this.inboundMsgPerSec = inboundMsgPerSec;
	}

	public int getAggregatorMsgInQue()
	{
		return aggregatorMsgInQue;
	}

	public void setAggregatorMsgInQue(int aggregatorMsgInQue)
	{
		this.aggregatorMsgInQue = aggregatorMsgInQue;
	}

	public int getAggregatorQueSize()
	{
		return aggregatorQueSize;
	}

	public void setAggregatorQueSize(int aggregatorQueSize)
	{
		this.aggregatorQueSize = aggregatorQueSize;
	}

	public int getAggregatorQueAge()
	{
		return aggregatorQueAge;
	}

	public void setAggregatorQueAge(int aggregatorQueAge)
	{
		this.aggregatorQueAge = aggregatorQueAge;
	}

	public int getStorageWriterMsgPerSec()
	{
		return storageWriterMsgPerSec;
	}

	public void setStorageWriterMsgPerSec(int storageWriterMsgPerSec)
	{
		this.storageWriterMsgPerSec = storageWriterMsgPerSec;
	}

	public int getStorageWriterBytesPerSec()
	{
		return storageWriterBytesPerSec;
	}

	public void setStorageWriterBytesPerSec(int storageWriterBytesPerSec)
	{
		this.storageWriterBytesPerSec = storageWriterBytesPerSec;
	}

	public int getStorageWriterBatchesBeingWritten()
	{
		return storageWriterBatchesBeingWritten;
	}

	public void setStorageWriterBatchesBeingWritten(int storageWriterBatchesBeingWritten)
	{
		this.storageWriterBatchesBeingWritten = storageWriterBatchesBeingWritten;
	}

	public int getOutboundMsgPerSec()
	{
		return outboundMsgPerSec;
	}

	public void setOutboundMsgPerSec(int outboundMsgPerSec)
	{
		this.outboundMsgPerSec = outboundMsgPerSec;
	}

	public int getOutboundAvgMsgSize()
	{
		return outboundAvgMsgSize;
	}

	public void setOutboundAvgMsgSize(int outboundAvgMsgSize)
	{
		this.outboundAvgMsgSize = outboundAvgMsgSize;
	}

	public int getOutboundActiveQueries()
	{
		return outboundActiveQueries;
	}

	public void setOutboundActiveQueries(int outboundActiveQueries)
	{
		this.outboundActiveQueries = outboundActiveQueries;
	}
}
