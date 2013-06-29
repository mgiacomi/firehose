package com.gltech.scale.core.aggregator;

import com.gltech.scale.core.model.ChannelMetaData;
import org.joda.time.DateTime;
import org.junit.Test;

public class BatchNIOFileTest
{
	@Test
	public void testWriteRawBytes() throws Exception
	{
		BatchNIOFile cfc = new BatchNIOFile(new ChannelMetaData("test", ChannelMetaData.TTL_DAY, false), DateTime.now());
		printByteArray(1, cfc.getRawVarint32(1));
		printByteArray(12, cfc.getRawVarint32(12));
		printByteArray(134, cfc.getRawVarint32(134));
		printByteArray(1567, cfc.getRawVarint32(1567));
		printByteArray(15670, cfc.getRawVarint32(15670));
		printByteArray(156700, cfc.getRawVarint32(156700));
		printByteArray(1567000, cfc.getRawVarint32(1567000));
		printByteArray(156700000, cfc.getRawVarint32(156700000));
		printByteArray(1567000000, cfc.getRawVarint32(1567000000));
		printByteArray(Integer.MAX_VALUE, cfc.getRawVarint32(Integer.MAX_VALUE));
	}

	private void printByteArray(int num, byte[] data)
	{
		/*
				 System.out.print(num +" : ");
				 for(byte b : data) {
				 System.out.print(Integer.toBinaryString(0x100 + b).substring(1) +" ");
				 }
				 System.out.println(" : "+ data.length);
				 */
	}
}
