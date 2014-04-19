package com.thinkaurelius.titan.diskstorage.cassandra.cql;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NAMESPACE;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.diskstorage.StorageException;
import org.junit.BeforeClass;

public class CqlKeyColumnValueStoreTest extends AbstractCassandraKeyColumnValueStoreTest {
    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

    @Override
    public Configuration getBaseStorageConfiguration() {
        return CassandraStorageSetup.getGenericCassandraStorageConfiguration(getClass().getSimpleName());
    }

    @Override
    public AbstractCassandraStoreManager openStorageManager(Configuration config) throws StorageException {
        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_BACKEND_KEY, "cassandracql");
        return new CqlStoreManager(config);
    }
}
