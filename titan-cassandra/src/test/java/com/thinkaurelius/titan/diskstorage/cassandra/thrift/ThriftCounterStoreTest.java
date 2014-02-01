package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraCounterStoreTest;

public class ThriftCounterStoreTest extends AbstractCassandraCounterStoreTest {
    public ThriftCounterStoreTest() throws StorageException {
        super(new CassandraThriftStoreManager(getBaseStorageConfiguration(ThriftCounterStoreTest.class.getSimpleName())).openCounters("THRIFT_COUNTERS"));
    }
}
