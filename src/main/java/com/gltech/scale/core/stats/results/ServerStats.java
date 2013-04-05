package com.gltech.scale.core.stats.results;

import com.dyuproject.protostuff.Tag;

import java.util.HashSet;
import java.util.Set;

public class ServerStats
{
	@Tag(1)
	private String workerId;
	@Tag(2)
	private Set<GroupStats> groupStatsList = new HashSet<>();

	public String getWorkerId()
	{
		return workerId;
	}

	public void setWorkerId(String workerId)
	{
		this.workerId = workerId;
	}

	public Set<GroupStats> getGroupStatsList()
	{
		return groupStatsList;
	}

	public void setGroupStatsList(Set<GroupStats> groupStatsList)
	{
		this.groupStatsList = groupStatsList;
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ServerStats that = (ServerStats) o;

		if (workerId != null ? !workerId.equals(that.workerId) : that.workerId != null) return false;

		return true;
	}

	public int hashCode()
	{
		return workerId != null ? workerId.hashCode() : 0;
	}
}
