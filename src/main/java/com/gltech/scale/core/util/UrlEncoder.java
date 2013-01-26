package com.gltech.scale.core.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class UrlEncoder
{

	public static String encode(String toEncode)
	{
		try
		{
			return URLEncoder.encode(toEncode, "UTF-8");
		}
		catch (UnsupportedEncodingException e)
		{
			return toEncode;
		}
	}

}
