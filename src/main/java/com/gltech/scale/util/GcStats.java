package com.gltech.scale.util;

import com.sun.management.GcInfo;

import javax.management.MBeanServer;

import com.sun.management.GarbageCollectorMXBean;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class GcStats
{
	private static final String GC_BEAN_NAME = "java.lang:type=GarbageCollector,name=MarkSweepCompact";
	private static GarbageCollectorMXBean gcMBean;


	public GcStats()
	{
		MBeanServer server = ManagementFactory.getPlatformMBeanServer();

		Set names = server.queryNames(null, null);
		System.out.println(names.toString().replace(", ",
				System.getProperty("line.separator")));
		try
		{
			gcMBean = ManagementFactory.newPlatformMXBeanProxy(server, GC_BEAN_NAME, GarbageCollectorMXBean.class);
		}
		catch (IOException e)
		{
		}
	}

	public boolean printGCInfo()
	{
		try
		{
			GcInfo gci = gcMBean.getLastGcInfo();
			long id = gci.getId();
			long startTime = gci.getStartTime();
			long endTime = gci.getEndTime();
			long duration = gci.getDuration();

			if (startTime == endTime)
			{
				return false;   // no gc
			}
			System.out.println("GC ID: " + id);
			System.out.println("Start Time: " + startTime);
			System.out.println("End Time: " + endTime);
			System.out.println("Duration: " + duration);
			Map<String, MemoryUsage> mapBefore = gci.getMemoryUsageBeforeGc();
			Map<String, MemoryUsage> mapAfter = gci.getMemoryUsageAfterGc();

			System.out.println("Before GC Memory Usage Details....");
			Set memType = mapBefore.keySet();
			Iterator it = memType.iterator();
			while (it.hasNext())
			{
				String type = (String) it.next();
				System.out.println(type);
				MemoryUsage mu1 = mapBefore.get(type);
				System.out.print("Initial Size: " + mu1.getInit());
				System.out.print(" Used: " + mu1.getUsed());
				System.out.print(" Max: " + mu1.getMax());
				System.out.print(" Committed: " + mu1.getCommitted());
				System.out.println(" ");
			}

			System.out.println("After GC Memory Usage Details....");
			memType = mapAfter.keySet();
			it = memType.iterator();
			while (it.hasNext())
			{
				String type = (String) it.next();
				System.out.println(type);
				MemoryUsage mu2 = mapAfter.get(type);
				System.out.print("Initial Size: " + mu2.getInit());
				System.out.print(" Used: " + mu2.getUsed());
				System.out.print(" Max: " + mu2.getMax());
				System.out.print(" Committed: " + mu2.getCommitted());
				System.out.println(" ");
			}
		}
		catch (RuntimeException re)
		{
			throw re;
		}
		catch (Exception exp)
		{
			throw new RuntimeException(exp);
		}
		return true;
	}
}
