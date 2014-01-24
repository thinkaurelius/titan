package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import com.thinkaurelius.titan.diskstorage.CounterStoreTest;

public class ThriftCounterStoreTest extends CounterStoreTest {
    public ThriftCounterStoreTest() {
        super(new CassandraThriftCounterStore("", "", null));
    }
}
