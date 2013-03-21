package com.gltech.scale.util;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class StreamDelimiterTest
{
	@Test
	public void testCodedStreams() throws Exception
	{
		String shortStr = "123";
		String medStr = "asdfqwreasdfqweradfqewrasdfqweradfqweradsfqwerasdfqwerafdsqwre";
		String longStr = "298347012357091735409813750917345908173450987132459087129430712904371920834" +
				"091872349871249871239487129347129437192479182347918274918273491723498172497124397124" +
				"987123947812493719824791579183275917325498173254981732549871239584719548719283472194";
		String nullStr = "";

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(bos);
		codedOutputStream.writeRawVarint32(shortStr.length());
		codedOutputStream.writeRawBytes(shortStr.getBytes());
		codedOutputStream.writeRawVarint32(medStr.length());
		codedOutputStream.writeRawBytes(medStr.getBytes());
		codedOutputStream.writeRawVarint32(nullStr.length());
		codedOutputStream.writeRawBytes(nullStr.getBytes());
		codedOutputStream.writeRawVarint32(longStr.length());
		codedOutputStream.writeRawBytes(longStr.getBytes());
		codedOutputStream.flush();

		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		CodedInputStream codedInputStream = CodedInputStream.newInstance(bis);

		Assert.assertEquals(shortStr, new String(codedInputStream.readRawBytes(codedInputStream.readRawVarint32())));
		Assert.assertFalse(codedInputStream.isAtEnd());
		Assert.assertEquals(medStr, new String(codedInputStream.readRawBytes(codedInputStream.readRawVarint32())));
		Assert.assertFalse(codedInputStream.isAtEnd());
		Assert.assertEquals(nullStr, new String(codedInputStream.readRawBytes(codedInputStream.readRawVarint32())));
		Assert.assertFalse(codedInputStream.isAtEnd());
		Assert.assertEquals(longStr, new String(codedInputStream.readRawBytes(codedInputStream.readRawVarint32())));
		Assert.assertTrue(codedInputStream.isAtEnd());
	}
}
