package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.testcategory.RandomPartitionerTests;

@Category({RandomPartitionerTests.class})
public class InternalCassandraThriftKeyColumnValueTest extends AbstractCassandraKeyColumnValueStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraProcessStarter.startCleanEmbedded(CassandraStorageSetup.cassandraYamlPath);
    }

    @Override
    public AbstractCassandraStoreManager openStorageManager() throws StorageException {
        Configuration sc = CassandraStorageSetup.getGenericCassandraStorageConfiguration(getClass().getSimpleName());
        return new CassandraThriftStoreManager(sc);
    }
}
