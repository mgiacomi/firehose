package com.gltech.scale.core.util;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class Props
{
	private static final Logger logger = LoggerFactory.getLogger(Props.class);

	private static final Props props = new Props();

	private ConcurrentHashMap<String, String> propertiesMap = new ConcurrentHashMap<>();

	private boolean verbose = false;

	public static Props getProps()
	{
		return props;
	}

	public void setVerbose(boolean verbose)
	{
		logger.info("setting verbosity to " + verbose + " at " + new Exception().getStackTrace()[1]);
		this.verbose = verbose;
	}

	public void loadFromFile(String filename)
	{
		logVerbose("loading from file " + filename);
		try (FileInputStream inputStream = new FileInputStream(filename))
		{
			loadFromInputStream(inputStream);
		}
		catch (IOException e)
		{
			logger.error("FileNotFoundException occurred loading properties from " + filename, e);
			throw new RuntimeException("FileNotFoundException occurred in loadFromFile", e);
		}
	}

	public void loadFromInputStream(InputStream inStream)
	{
		Properties properties = new Properties();
		try
		{
			properties.load(inStream);
		}
		catch (IOException e)
		{
			logger.error("unable to load stream", e);
			throw new RuntimeException("unable to load stream", e);
		}

		for (Map.Entry<Object, Object> entry : properties.entrySet())
		{
			String key = (String) entry.getKey();
			String value = (String) entry.getValue();
			logVerbose("setting from stream - key '{}' to value '{} ", key, value);
			propertiesMap.put(key, value);
		}
	}

	public void set(String key, String value)
	{
		logVerbose("setting key '{}' to value '{} ", key, value);
		propertiesMap.put(key, value);
	}

	public void set(String key, int value)
	{
		set(key, Integer.toString(value));
	}

	public void set(String key, boolean value)
	{
		set(key, Boolean.toString(value));
	}

	public String get(String key, String defaultValue)
	{
		String value = propertiesMap.get(key);
		if (null != value)
		{
			logVerbose("found value '{}' for key '{}'", value, key);
			return value;
		}
		logVerbose("returning default value '{}' for key '{}'", defaultValue, key);
		return defaultValue;
	}

	public int get(String key, int defaultValue)
	{
		String value = get(key, Integer.toString(defaultValue));
		try
		{
			return Integer.parseInt(value);
		}
		catch (NumberFormatException e)
		{
			logger.warn("value for " + key + " was " + value + ", and not a valid Integer");
			return defaultValue;
		}
	}

	public boolean get(String key, boolean defaultValue)
	{
		String value = get(key, Boolean.toString(defaultValue));
		return Boolean.parseBoolean(value);
	}

	private void logVerbose(String text)
	{
		if (verbose)
		{
			logger.info(text);
		}
	}

	private void logVerbose(String text, Object... objects)
	{
		if (verbose)
		{
			logger.info(text, objects);
		}
	}

	public String toString()
	{
		return "Props {" +
				"propertiesMap=" + propertiesMap +
				'}';
	}

	public List<String> get(String key, List<String> defaultList)
	{
		String found = get(key, (String) null);
		if (null == found)
		{
			return defaultList;
		}
		return Arrays.asList(StringUtils.split(found, ";"));
	}

	public void set(String key, List<String> list)
	{
		set(key, StringUtils.join(list, ";"));
	}
}
