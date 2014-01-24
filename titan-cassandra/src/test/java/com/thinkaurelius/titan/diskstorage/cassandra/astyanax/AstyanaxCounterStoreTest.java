package com.thinkaurelius.titan.diskstorage.cassandra.astyanax;

import com.thinkaurelius.titan.diskstorage.CounterStoreTest;

public class AstyanaxCounterStoreTest extends CounterStoreTest {
    public AstyanaxCounterStoreTest() {
        super(new CassandraAstyanaxCounterStore(null, ""));
    }
}
