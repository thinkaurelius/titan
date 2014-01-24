package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.diskstorage.CounterStoreTest;

public class HBaseCounterStoreTest extends CounterStoreTest {
    public HBaseCounterStoreTest() {
        super(new HBaseCounterStore(null, null, "", ""));
    }
}
