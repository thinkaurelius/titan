package com.thinkaurelius.titan.diskstorage.berkeleyje;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.berkeleydb.je.BerkeleyJEStorageManagerFactory;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public final class BerkeleyDBjeStorageSetup {

    private BerkeleyDBjeStorageSetup() {
        // Util class cannot be instantiated
    }

    public static Configuration getBerkeleyJEStorageConfiguration() {
        return StorageSetup.getLocalStorageConfiguration();
    }

    public static Configuration getBerkeleyJEGraphConfiguration() {
        Configuration config = StorageSetup.getLocalGraphConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,BerkeleyJEStorageManagerFactory.BERKELEYJE);
        return config;
    }
    
}
