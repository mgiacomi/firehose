package com.gltech.scale.monitoring.model;

import com.dyuproject.protostuff.Tag;
import com.gltech.scale.core.stats.results.GroupStats;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ServerStats
{
	@Tag(1)
	private String workerId;
	@Tag(2)
	private String hostname;
	@Tag(3)
	private Set<String> roles = new HashSet<>();
	@Tag(4)
	private String joinDate;
	@Tag(5)
	private String status;
	@Tag(6)
	private Map<String, GroupStats> groupStatsList = new HashMap<>();

	public String getWorkerId()
	{
		return workerId;
	}

	public void setWorkerId(String workerId)
	{
		this.workerId = workerId;
	}

	public String getHostname()
	{
		return hostname;
	}

	public void setHostname(String hostname)
	{
		this.hostname = hostname;
	}

	public  Map<String, GroupStats> getGroupStatsList()
	{
		return groupStatsList;
	}

	public void setGroupStatsList(Map<String, GroupStats> groupStatsList)
	{
		this.groupStatsList = groupStatsList;
	}

	public String getJoinDate()
	{
		return joinDate;
	}

	public void setJoinDate(String joinDate)
	{
		this.joinDate = joinDate;
	}

	public String getStatus()
	{
		return status;
	}

	public void setStatus(String status)
	{
		this.status = status;
	}

	public Set<String> getRoles()
	{
		return roles;
	}

	public void setRoles(Set<String> roles)
	{
		this.roles = roles;
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
