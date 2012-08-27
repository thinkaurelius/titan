package com.thinkaurelius.titan.diskstorage.astyanax;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraThriftStorageManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class ExternalAstyanaxLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {
	
    @Override
    public StorageManager openStorageManager(short idx) {
    	Configuration sc = CassandraStorageSetup.getCassandraStorageConfiguration();
    	sc.addProperty(CassandraThriftStorageManager.LOCAL_LOCK_MEDIATOR_PREFIX_KEY, "astyanax-" + idx);
    	sc.addProperty(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY, idx);
    	
        return new AstyanaxStorageManager(sc);
    }
}
