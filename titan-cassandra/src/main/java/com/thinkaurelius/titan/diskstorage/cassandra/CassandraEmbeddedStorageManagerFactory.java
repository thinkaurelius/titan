package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.graphdb.configuration.AbstractStorageManagerService;

public class CassandraEmbeddedStorageManagerFactory extends AbstractStorageManagerService<CassandraEmbeddedStorageManager> {

    public static final String EMBEDDED_CASSANDRA = "embeddedcassandra";

    @Override
    public Class<CassandraEmbeddedStorageManager> getStorageManagerClass() {
        return CassandraEmbeddedStorageManager.class;
    }

    @Override
    public String getName() {
        return EMBEDDED_CASSANDRA;
    }

}
