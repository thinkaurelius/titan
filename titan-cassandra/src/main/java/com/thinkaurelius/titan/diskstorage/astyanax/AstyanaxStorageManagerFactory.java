package com.thinkaurelius.titan.diskstorage.astyanax;

import com.thinkaurelius.titan.graphdb.configuration.AbstractStorageManagerService;

public class AstyanaxStorageManagerFactory extends AbstractStorageManagerService<AstyanaxStorageManager> {

	public static final String ASTYANAX = "astyanax";

    @Override
	public Class<AstyanaxStorageManager> getStorageManagerClass() {
		return AstyanaxStorageManager.class;
	}

    @Override
    public String getName() {
        return ASTYANAX;
    }

}
