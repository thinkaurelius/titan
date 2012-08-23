package com.thinkaurelius.titan.diskstorage.cassandra;

import com.thinkaurelius.titan.graphdb.configuration.AbstractStorageManagerService;

public class CassandraThriftStorageManagerFactory extends AbstractStorageManagerService<CassandraThriftStorageManager> {

	@Override
	public Class<CassandraThriftStorageManager> getStorageManagerClass() {
		return CassandraThriftStorageManager.class;
	}

    @Override
    public String getName() {
        return "cassandra";
    }

}
