package com.thinkaurelius.titan.diskstorage.inmemory;

import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.KeyValueStoreUtil;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.Before;

/**
 * @author janar@
 */
public class InMemoryKeyColumnValueStoreUTF8Test extends InMemoryKeyColumnValueStoreTest {

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        KeyValueStoreUtil.setUTF8Serializer();
    }

    @After
    public void tearDown() throws Exception {
        KeyValueStoreUtil.setDefaultSerializer();
    }

}

