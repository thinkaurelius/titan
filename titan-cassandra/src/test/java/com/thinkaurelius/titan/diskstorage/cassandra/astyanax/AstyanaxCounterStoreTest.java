package com.thinkaurelius.titan.diskstorage.cassandra.astyanax;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraCounterStoreTest;

public class AstyanaxCounterStoreTest extends AbstractCassandraCounterStoreTest {
    public AstyanaxCounterStoreTest() throws StorageException {
        super(new AstyanaxStoreManager(getBaseStorageConfiguration(AstyanaxCounterStoreTest.class.getSimpleName())).openCounters("ASTYANAX_COUNTERS"));
    }
}
