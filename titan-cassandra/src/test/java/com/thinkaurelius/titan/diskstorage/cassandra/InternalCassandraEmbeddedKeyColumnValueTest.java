package com.thinkaurelius.titan.diskstorage.cassandra;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;

public class InternalCassandraEmbeddedKeyColumnValueTest extends KeyColumnValueStoreTest {

    @Override
    public StorageManager openStorageManager() {
        return new CassandraEmbeddedStorageManager(getConfiguration());
    }

    private Configuration getConfiguration() {
        Configuration config = CassandraStorageSetup.getEmbeddedCassandraStorageConfiguration();
        return config;
    }
}
