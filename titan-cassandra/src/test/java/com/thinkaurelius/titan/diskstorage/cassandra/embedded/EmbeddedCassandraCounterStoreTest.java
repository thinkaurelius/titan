package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import com.thinkaurelius.titan.diskstorage.CounterStoreTest;

public class EmbeddedCassandraCounterStoreTest extends CounterStoreTest {
    public EmbeddedCassandraCounterStoreTest() {
        super(new CassandraEmbeddedCounterStore("test_counters_ks", "counters"));
    }
}
