package com.thinkaurelius.titan.diskstorage.cassandra;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.diskstorage.LockKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class InternalCassandraEmbeddedLockKeyColumnValueStoreTest extends LockKeyColumnValueStoreTest {
    
    @Override
    public StorageManager openStorageManager(short idx) {
    	Configuration sc = CassandraStorageSetup.getEmbeddedCassandraStorageConfiguration();
    	sc.addProperty(CassandraThriftStorageManager.LOCAL_LOCK_MEDIATOR_PREFIX_KEY, "cassandra-" + idx);
    	sc.addProperty(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY, idx);
    	
        return new CassandraEmbeddedStorageManager(sc);
    }
}
