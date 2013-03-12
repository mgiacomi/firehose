package com.gltech.scale.ganglia;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class StatisticsFilter implements Filter
{
	private static final Logger logger = LoggerFactory.getLogger(StatisticsFilter.class);

	private static final List<String> paths = new ArrayList<>();
	private static final Map<String, String> names = new HashMap<>();
	private static TimerMap timerMap;

	public static void add(TimerMap timerMap)
	{

		StatisticsFilter.timerMap = timerMap;
	}

	public static void add(String path, String name)
	{
		paths.add(path);
		names.put(path, name);
	}

	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException
	{
		long start = System.nanoTime();
		filterChain.doFilter(servletRequest, servletResponse);
		if (!HttpServletRequest.class.isAssignableFrom(servletRequest.getClass()))
		{
			return;
		}
		long nanos = System.nanoTime() - start;
		HttpServletRequest request = (HttpServletRequest) servletRequest;
		String servletPath = request.getServletPath();
		if (null == servletPath)
		{
			return;
		}
		for (String path : paths)
		{
			if (servletPath.contains(path))
			{
				String name = names.get(path) + "." + request.getMethod();
				timerMap.get(name).add(nanos);
				return;
			}
		}

	}

	public void init(FilterConfig filterConfig) throws ServletException
	{
	}

	public void destroy()
	{
	}
}
