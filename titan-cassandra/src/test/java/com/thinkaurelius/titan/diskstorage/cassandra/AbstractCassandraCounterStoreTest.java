package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.CounterStoreTest;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnCounterStore;
import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

public abstract class AbstractCassandraCounterStoreTest extends CounterStoreTest {
    public AbstractCassandraCounterStoreTest(KeyColumnCounterStore store) {
        super(store);
    }

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    public static Configuration getBaseStorageConfiguration(String keyspaceName) {
        return CassandraStorageSetup.getGenericCassandraStorageConfiguration(keyspaceName);
    }
}
