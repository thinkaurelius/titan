package com.thinkaurelius.titan.diskstorage.astyanax;

import org.junit.BeforeClass;

import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraDaemonWrapper;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraStorageSetup;

public class InternalAstyanaxMultiWriteKeyColumnValueTest extends MultiWriteKeyColumnValueStoreTest {

	@BeforeClass
	public static void startCassandra() {
    	CassandraDaemonWrapper.start(CassandraStorageSetup.cassandraYamlPath);
	}
	
    @Override
    public StorageManager openStorageManager() {
        return new AstyanaxStorageManager(CassandraStorageSetup.getCassandraStorageConfiguration());
    }
}
