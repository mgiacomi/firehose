package com.gltech.scale.core.cluster;

public class WeightBreakdown
{
	private boolean active;
	private int primaries;
	private int backups;
	private int atRest;

	public WeightBreakdown(long weight)
	{
		String weightStr = String.valueOf(weight);

		if (weightStr.substring(0, 1).equals("2"))
		{
			active = true;
		}

		primaries = Integer.valueOf(weightStr.substring(1, 4));
		backups = Integer.valueOf(weightStr.substring(4, 7));
		atRest = Integer.valueOf(weightStr.substring(7, 10));
	}

	public WeightBreakdown(boolean active, int primaries, int backups, int atRest)
	{
		this.active = active;
		this.primaries = primaries;
		this.backups = backups;
		this.atRest = atRest;
	}

	public long toWeight()
	{
		StringBuilder weight = new StringBuilder();

		if (active)
		{
			weight.append(2);
		}
		else
		{
			weight.append(1);
		}

		weight.append(String.format("%03d", primaries));
		weight.append(String.format("%03d", backups));
		weight.append(String.format("%03d", atRest));

		return Long.valueOf(weight.toString());
	}

	public boolean isActive()
	{
		return active;
	}

	public int getPrimaries()
	{
		return primaries;
	}

	public void incrementPrimary()
	{
		primaries++;
	}

	public int getBackups()
	{
		return backups;
	}

	public void incrementBackup()
	{
		backups++;
	}

	public int getAtRest()
	{
		return atRest;
	}
}
