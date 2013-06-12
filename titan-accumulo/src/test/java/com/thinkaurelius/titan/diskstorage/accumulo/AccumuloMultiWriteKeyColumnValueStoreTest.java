package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.junit.BeforeClass;

import java.io.IOException;
import org.apache.commons.configuration.Configuration;

public class AccumuloMultiWriteKeyColumnValueStoreTest extends MultiWriteKeyColumnValueStoreTest {

    @BeforeClass
    public static void startAccumulo() throws IOException {
        AccumuloStorageSetup.startAccumulo();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        Configuration sc = AccumuloStorageSetup.getAccumuloStorageConfiguration();
        return new AccumuloStoreManager(sc);
    }
}
