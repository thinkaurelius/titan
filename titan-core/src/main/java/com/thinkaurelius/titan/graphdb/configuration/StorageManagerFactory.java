package com.thinkaurelius.titan.graphdb.configuration;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.diskstorage.StorageManager;

public interface StorageManagerFactory<T extends StorageManager> {

    public String getName();
    
	public Class<T> getStorageManagerClass();

    public StorageManager createStorageManager(Configuration storageconfig);
	
}
