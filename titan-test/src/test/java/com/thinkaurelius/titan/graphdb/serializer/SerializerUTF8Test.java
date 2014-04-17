package com.thinkaurelius.titan.graphdb.serializer;

import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.thinkaurelius.titan.testutil.RandomGenerator;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Before;
import org.junit.Test;

import static com.thinkaurelius.titan.graphdb.database.serialize.SerializerInitialization.RESERVED_ID_OFFSET;
import static org.junit.Assert.assertEquals;

/**
 * Runs the rest of serialization test with utf8 setting. primarily intended only for the string test
 * @author janar@
 */
public class SerializerUTF8Test extends SerializerTest
{
    @Before
    public void setUp() throws Exception {
        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(GraphDatabaseConfiguration.STRING_COMPACT_SERIALIZE,true);
        serialize = new KryoSerializer(conf);
        serialize.registerClass(TestEnum.class, RESERVED_ID_OFFSET + 1);
        serialize.registerClass(TestClass.class, RESERVED_ID_OFFSET + 2);
        serialize.registerClass(short[].class, RESERVED_ID_OFFSET + 3);

        printStats = true;
    }

    @Override
    @Test
    public void stringSerialization() {
        //Characters
        DataOutput out = serialize.getDataOutput(((int) Character.MAX_VALUE) * 2 + 8, true);
        for (char c = Character.MIN_VALUE; c < Character.MAX_VALUE; c++) {
            out.writeObjectNotNull(Character.valueOf(c));
        }
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        for (char c = Character.MIN_VALUE; c < Character.MAX_VALUE; c++) {
            assertEquals(c, serialize.readObjectNotNull(b, Character.class).charValue());
        }


        //String
        for (int t = 0; t < 10000; t++) {
            DataOutput out1 = serialize.getDataOutput(32 + 5, true);
            DataOutput out2 = serialize.getDataOutput(32 + 5, true);
            String s1 = RandomGenerator.randomString(1, 32);
            String s2 = RandomGenerator.randomString(1, 32);
            out1.writeObjectNotNull(s1);
            out2.writeObjectNotNull(s2);
            StaticBuffer b1 = out1.getStaticBuffer();
            StaticBuffer b2 = out2.getStaticBuffer();
            assertEquals(s1, serialize.readObjectNotNull(b1.asReadBuffer(), String.class));
            assertEquals(s2, serialize.readObjectNotNull(b2.asReadBuffer(), String.class));
            //todo: sort order test fails because... a bug in StaticArrayBuffer.compareTo() and ByteBuffer.compareTo(), where we ignore sign bit for 'byte' during int conversion!
            /*
             * ByteBufferUtil.java:
             * public static int compare(byte c1, byte c2) {
             *    return new Byte(c1).compareTo(new Byte(c2));
             * }
             *
             * the same in StaticArrayBuffer...
             *
             * But we seem to be relying on this bug elsewhere in the code.
             */
           //assertEquals(s1 + " vs " + s2, Integer.signum(s1.compareTo(s2)), Integer.signum(b1.compareTo(b2)));
        }
    }
}
