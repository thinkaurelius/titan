package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraCounterStoreTest;
import org.apache.commons.configuration.Configuration;

public class EmbeddedCassandraCounterStoreTest extends AbstractCassandraCounterStoreTest {
    public EmbeddedCassandraCounterStoreTest() throws StorageException {
        super(new CassandraEmbeddedStoreManager(getBaseStorageConfiguration()).openCounters("EMBEDDED_COUNTERS"));
    }

    public static Configuration getBaseStorageConfiguration() {
        return CassandraStorageSetup.getEmbeddedCassandraStorageConfiguration(EmbeddedCassandraCounterStoreTest.class.getSimpleName());
    }
}
