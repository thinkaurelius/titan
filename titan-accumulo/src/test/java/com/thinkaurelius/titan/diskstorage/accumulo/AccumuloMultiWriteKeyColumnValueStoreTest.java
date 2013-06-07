package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

import java.io.IOException;

public class AccumuloMultiWriteKeyColumnValueStoreTest extends MultiWriteKeyColumnValueStoreTest {

    @BeforeClass
    public static void startAccumulo() throws IOException {
        AccumuloStorageSetup.startAccumulo();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new AccumuloStoreManager(getConfig());
    }

    private Configuration getConfig() {
        Configuration c = AccumuloStorageSetup.getAccumuloStorageConfiguration();
        return c;
    }
}
