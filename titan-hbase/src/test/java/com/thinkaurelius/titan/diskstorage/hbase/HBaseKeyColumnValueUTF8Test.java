package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.diskstorage.KeyValueStoreUtil;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.Before;

/**
 * @author janar@
 */
public class HBaseKeyColumnValueUTF8Test extends HBaseKeyColumnValueTest
{
    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        Configuration conf  = new PropertiesConfiguration();
        conf.setProperty(GraphDatabaseConfiguration.STRING_UTF_SERIALIZATION,true);
        KeyValueStoreUtil.serial = new KryoSerializer(conf);
    }

    @After
    public void tearDown() throws Exception
    {
        KeyValueStoreUtil.serial = new KryoSerializer();
    }
}
