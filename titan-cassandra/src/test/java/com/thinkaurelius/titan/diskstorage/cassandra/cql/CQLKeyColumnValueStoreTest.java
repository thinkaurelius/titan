package com.thinkaurelius.titan.diskstorage.cassandra.cql;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;

import com.thinkaurelius.titan.diskstorage.StorageException;
import org.junit.BeforeClass;

public class CQLKeyColumnValueStoreTest extends AbstractCassandraKeyColumnValueStoreTest {
    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    @Override
    public ModifiableConfiguration getBaseStorageConfiguration() {
        return CassandraStorageSetup.getCassandraCQLConfiguration(this.getClass().getSimpleName());
    }

    @Override
    public AbstractCassandraStoreManager openStorageManager(Configuration config) throws StorageException {
        return new CQLStoreManager(config);
    }
}
