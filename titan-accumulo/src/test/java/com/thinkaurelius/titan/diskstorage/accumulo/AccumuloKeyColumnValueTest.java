package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

import java.io.IOException;

public class AccumuloKeyColumnValueTest extends KeyColumnValueStoreTest {
    @BeforeClass
    public static void startAccmulo() throws IOException {
        AccumuloStorageSetup.startAccumulo();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new AccumuloStoreManager(getConfig());
    }

    private Configuration getConfig() {
        Configuration c = AccumuloStorageSetup.getAccumuloStorageConfiguration();
        c.setProperty("accumulo-config.accumulo.zookeeper.quorum", "localhost");
        c.setProperty("accumulo-config.accumulo.zookeeper.property.clientPort", "2181");
        return c;
    }
}
