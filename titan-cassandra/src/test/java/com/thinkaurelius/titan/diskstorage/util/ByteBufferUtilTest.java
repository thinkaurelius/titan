package com.thinkaurelius.titan.diskstorage.util;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

/*
 * This test is really testing the ByteBufferUtil.bytesToHex(byteBuffer) method
 * from titan-core, but since it is replacing the same method from Cassandra's
 * org.apache.cassandra.utils.ByteBufferUtil the test resides here to bring in
 * the dependency.
 */
public class ByteBufferUtilTest {

	@Test
	public void testBytesToHex() {
		final byte[] bytes = "Testhatsnhoenutahoeu9aoe7u89oe7au89aeou897892gchuteonhuaaetnosuh"
				.getBytes();
		final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
		byteBuffer.position(20);
		byteBuffer.mark();
		byteBuffer.position(2);
		final String cassandraByteBufferHex = ByteBufferUtil.bytesToHex(byteBuffer);
		final String titanByteBufferHex =com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil.bytesToHex(byteBuffer);
		assertEquals(cassandraByteBufferHex, titanByteBufferHex);
	}

}
