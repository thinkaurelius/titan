package com.thinkaurelius.titan;

import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.KEYSPACE_KEY;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

import java.io.File;

public class CassandraStorageSetup {

    public static final String CASSANDRA_TEMP_PATH = System.getProperty("user.dir")
                                                     + File.separator + "target"
                                                     + File.separator + "cassandra-temp";

    public static final String cassandraYamlPath = StringUtils.join(
            new String[]{"file://", System.getProperty("user.dir"), "target",
                    "cassandra-tmp", "conf", "127.0.0.1", "cassandra.yaml"},
            File.separator);
    public static final String cassandraOrderedYamlPath = StringUtils.join(
            new String[]{"file://", System.getProperty("user.dir"), "target",
                    "cassandra-tmp", "conf", "127.0.0.1", "cassandra-ordered.yaml"},
            File.separator);


    public static Configuration getGenericCassandraStorageConfiguration(String ks) {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(KEYSPACE_KEY, cleanKeyspaceName(ks));
        return config;
    }

    public static Configuration getAstyanaxGraphConfiguration(String ks) {
        return getGraphBaseConfiguration(ks, "astyanax");
    }

    public static Configuration getCassandraGraphConfiguration(String ks) {
        return getGraphBaseConfiguration(ks, "cassandra");
    }

    public static Configuration getCassandraThriftGraphConfiguration(String ks) {
        return getGraphBaseConfiguration(ks, "cassandrathrift");
    }
    
    /*
     * Cassandra only accepts keyspace names 48 characters long or shorter made
     * up of alphanumeric characters and underscores.
     */
    private static String cleanKeyspaceName(String raw) {
        Preconditions.checkNotNull(raw);
        Preconditions.checkArgument(0 < raw.length());
        
        if (48 < raw.length() || raw.matches("[^a-zA-Z_]")) {
            return "strhash" + String.valueOf(Math.abs(raw.hashCode()));
        } else {
            return raw;
        }
    }
    
    private static Configuration getGraphBaseConfiguration(String ks, String backend) {
        Configuration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(KEYSPACE_KEY, cleanKeyspaceName(ks));
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, backend);
        return config;
    }
}
