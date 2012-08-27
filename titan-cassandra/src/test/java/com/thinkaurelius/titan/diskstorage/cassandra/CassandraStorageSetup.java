package com.thinkaurelius.titan.diskstorage.cassandra;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.astyanax.AstyanaxStorageManagerFactory;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public final class CassandraStorageSetup {

    private CassandraStorageSetup() {
        // Util class cannot be instantiated
    }

    public static final String cassandraYamlPath = StringUtils.join(
            new String[] { "file://", System.getProperty("user.dir"), "target", "cassandra-tmp", "conf", "127.0.0.1",
                    "cassandra.yaml" }, File.separator);

    public static Configuration getCassandraStorageConfiguration() {
        Configuration config = StorageSetup.getLocalStorageConfiguration();
        return config;
    }

    public static Configuration getEmbeddedCassandraStorageConfiguration() {
        Configuration config = StorageSetup.getLocalStorageConfiguration();
        config.addProperty(
        		CassandraEmbeddedStorageManager.CASSANDRA_CONFIG_DIR_KEY,
        		cassandraYamlPath);
        return config;
    }

    public static Configuration getAstyanaxGraphConfiguration() {
        Configuration config = StorageSetup.getLocalGraphConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(
                GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, AstyanaxStorageManagerFactory.ASTYANAX);
        return config;
    }

    public static Configuration getCassandraGraphConfiguration() {
        Configuration config = StorageSetup.getLocalGraphConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(
                GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, CassandraThriftStorageManagerFactory.CASSANDRA);
        return config;
    }

    public static Configuration getEmbeddedCassandraGraphConfiguration() {
        Configuration config = StorageSetup.getLocalGraphConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(
                GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, CassandraEmbeddedStorageManagerFactory.EMBEDDED_CASSANDRA);
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(
                CassandraEmbeddedStorageManager.CASSANDRA_CONFIG_DIR_KEY, cassandraYamlPath);
        return config;
    }

}
