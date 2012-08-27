package com.thinkaurelius.titan.diskstorage.hbase;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class ExternalHBaseStorageSetup {

    public static Configuration getHBaseStorageConfiguration() {
        return StorageSetup.getLocalStorageConfiguration();
    }

    public static Configuration getHBaseGraphConfiguration() {
        Configuration config = StorageSetup.getLocalGraphConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,HBaseStorageManagerFactory.HBASE);
        return config;
    }

}
