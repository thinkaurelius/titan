package com.thinkaurelius.titan.diskstorage.berkeleydb.je;

import com.thinkaurelius.titan.graphdb.configuration.AbstractStorageManagerService;

public class BerkeleyJEStorageManagerFactory extends AbstractStorageManagerService<BerkeleyJEStorageAdapter> {

	public static final String BERKELEYJE = "berkeleyje";

    @Override
	public Class<BerkeleyJEStorageAdapter> getStorageManagerClass() {
		return BerkeleyJEStorageAdapter.class;
	}

    @Override
    public String getName() {
        return BERKELEYJE;
    }

}
