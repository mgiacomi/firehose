package com.gltech.scale.core.cluster.registration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public class ServiceMetaData
{
	@JsonProperty("workerId")
	private UUID workerId;

	@JsonProperty("listenAddress")
	private String listenAddress;

	@JsonProperty("listenPort")
	private int listenPort;

	public ServiceMetaData()
	{
	}

	@JsonCreator
	public ServiceMetaData(@JsonProperty("workerId") UUID workerId,
						   @JsonProperty("listenAddress") String listenAddress,
						   @JsonProperty("listenPort") int listenPort)
	{
		this.workerId = workerId;
		this.listenAddress = listenAddress;
		this.listenPort = listenPort;
	}

	public UUID getWorkerId()
	{
		return workerId;
	}

	public String getListenAddress()
	{
		return listenAddress;
	}

	public int getListenPort()
	{
		return listenPort;
	}

	public void setWorkerId(UUID workerId)
	{
		this.workerId = workerId;
	}

	public void setListenAddress(String listenAddress)
	{
		this.listenAddress = listenAddress;
	}

	public void setListenPort(int listenPort)
	{
		this.listenPort = listenPort;
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ServiceMetaData that = (ServiceMetaData) o;

		if (listenPort != that.listenPort) return false;
		if (listenAddress != null ? !listenAddress.equals(that.listenAddress) : that.listenAddress != null)
			return false;
		if (workerId != null ? !workerId.equals(that.workerId) : that.workerId != null) return false;

		return true;
	}

	public int hashCode()
	{
		int result = workerId != null ? workerId.hashCode() : 0;
		result = 31 * result + (listenAddress != null ? listenAddress.hashCode() : 0);
		result = 31 * result + listenPort;
		return result;
	}

	public String toString()
	{
		return "AggregatorMetaData{" +
				"workerId=" + workerId +
				", listenAddress='" + listenAddress + '\'' +
				", listenPort=" + listenPort +
				'}';
	}
}
