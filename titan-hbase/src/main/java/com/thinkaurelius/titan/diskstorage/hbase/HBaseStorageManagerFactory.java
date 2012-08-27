package com.thinkaurelius.titan.diskstorage.hbase;

import com.thinkaurelius.titan.graphdb.configuration.AbstractStorageManagerService;

public class HBaseStorageManagerFactory extends AbstractStorageManagerService<HBaseStorageManager> {

	public static final String HBASE = "hbase";

    @Override
	public Class<HBaseStorageManager> getStorageManagerClass() {
		return HBaseStorageManager.class;
	}

    @Override
    public String getName() {
        return HBASE;
    }

}
