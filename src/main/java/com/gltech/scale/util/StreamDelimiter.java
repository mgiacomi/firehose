package com.gltech.scale.util;

import com.google.common.base.Throwables;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamDelimiter
{
	private static final String ERR_TRUNCATED_MESSAGE =
			"While parsing a protocol message, the input ended unexpectedly " +
					"in the middle of a field.  This could mean either than the " +
					"input has been truncated or that an embedded message " +
					"misreported its own length.";

	private static final String ERR_MALFORMED_VARINT = "encountered a malformed varint.";

	static public void write(final OutputStream out, byte[] bytes)
	{
		try
		{
			if (bytes != null && bytes.length > 0)
			{
				writeRawVarInt32Bytes(out, bytes.length);
				out.write(bytes);
			}
		}
		catch (Exception e)
		{
			throw Throwables.propagate(e);
		}
	}

	static public byte[] readNext(final InputStream in) throws IOException
	{
		final int size = in.read();
		if (size == -1)
			throw new EOFException("End of stream");

		final int len = size < 0x80 ? size : readRawVarint32(in, size);
		if (len != 0)
		{
			byte[] bytes = new byte[len];

			int bytesRead = in.read(bytes);

			if (bytesRead < len)
			{
				throw new RuntimeException(ERR_TRUNCATED_MESSAGE);
			}

			return bytes;
		}

		throw new RuntimeException(ERR_TRUNCATED_MESSAGE);
	}

	private static void writeRawVarInt32Bytes(OutputStream out, int value) throws IOException
	{
		while (true)
		{
			if ((value & ~0x7F) == 0)
			{
				out.write(value);
				return;
			}
			else
			{
				out.write((value & 0x7F) | 0x80);
				value >>>= 7;
			}
		}
	}

	/**
	 * Reads a varint from the input one byte at a time, so that it does not
	 * read any bytes after the end of the varint.
	 */
	private static int readRawVarint32(final InputStream input, final int firstByte) throws IOException
	{
		int result = firstByte & 0x7f;
		int offset = 7;
		for (; offset < 32; offset += 7)
		{
			final int b = input.read();
			if (b == -1)
			{
				throw new RuntimeException(ERR_TRUNCATED_MESSAGE);
			}
			result |= (b & 0x7f) << offset;
			if ((b & 0x80) == 0)
			{
				return result;
			}
		}
		// Keep reading up to 64 bits.
		for (; offset < 64; offset += 7)
		{
			final int b = input.read();
			if (b == -1)
			{
				throw new RuntimeException(ERR_TRUNCATED_MESSAGE);
			}
			if ((b & 0x80) == 0)
			{
				return result;
			}
		}
		throw new RuntimeException(ERR_MALFORMED_VARINT);
	}
}
