package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.diskstorage.CounterStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import org.apache.commons.configuration.Configuration;

import org.junit.BeforeClass;

public class HBaseCounterStoreTest extends CounterStoreTest {
    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    private static Configuration getConfig() {
        Configuration c = HBaseStorageSetup.getHBaseStorageConfiguration();
        c.setProperty("hbase-config.hbase.zookeeper.quorum", "localhost");
        c.setProperty("hbase-config.hbase.zookeeper.property.clientPort", "2181");
        return c;
    }

    public HBaseCounterStoreTest() throws StorageException {
        super(new HBaseStoreManager(getConfig()).openCounters("HBASE_COUNTERS_TEST"));
    }
}
