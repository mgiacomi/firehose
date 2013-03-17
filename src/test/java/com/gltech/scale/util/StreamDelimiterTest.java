package com.gltech.scale.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;

public class StreamDelimiterTest
{
	@Test
	public void testStream() throws Exception
	{
		StreamDelimiter streamDelimiter = new StreamDelimiter();

		String shortStr = "123";
		String medStr = "asdfqwreasdfqweradfqewrasdfqweradfqweradsfqwerasdfqwerafdsqwre";
		String longStr = "298347012357091735409813750917345908173450987132459087129430712904371920834" +
				"091872349871249871239487129347129437192479182347918274918273491723498172497124397124" +
				"987123947812493719824791579183275917325498173254981732549871239584719548719283472194";
		String nullStr = "";

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		streamDelimiter.write(bos, shortStr.getBytes());
		streamDelimiter.write(bos, null);
		streamDelimiter.write(bos, medStr.getBytes());
		streamDelimiter.write(bos, nullStr.getBytes());
		streamDelimiter.write(bos, longStr.getBytes());

		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		Assert.assertEquals(shortStr, new String(streamDelimiter.readNext(bis)));
		Assert.assertEquals(medStr, new String(streamDelimiter.readNext(bis)));
		Assert.assertEquals(longStr, new String(streamDelimiter.readNext(bis)));

		boolean gotEOF = false;
		try
		{
			streamDelimiter.readNext(bis);
		}
		catch(EOFException e) {
			gotEOF = true;
		}

		Assert.assertTrue(gotEOF);
	}
}
