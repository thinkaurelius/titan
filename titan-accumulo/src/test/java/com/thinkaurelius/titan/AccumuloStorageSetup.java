package com.thinkaurelius.titan;

import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import java.io.IOException;
import java.lang.reflect.Constructor;

public class AccumuloStorageSetup {

    public static AccumuloStoreManager getAccumuloStoreManager() throws StorageException {
        try {
            Configuration config = getAccumuloStorageConfiguration();
            String backend = config.getString(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY);

            Class cls = Class.forName(backend);
            Constructor constructor = cls.getDeclaredConstructor(Configuration.class);

            return (AccumuloStoreManager) constructor.newInstance(config);
        } catch (Exception ex) {
            throw new PermanentStorageException(ex);
        }
    }

    public static Configuration getAccumuloStorageConfiguration() {
        return getAccumuloGraphConfiguration()
                .subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
    }

    public static Configuration getAccumuloGraphConfiguration() {
        BaseConfiguration config = new BaseConfiguration();

        Configuration storageConfig = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);

        storageConfig.addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,
                "com.thinkaurelius.titan.diskstorage.accumulo.MockAccumuloStoreManager");
        storageConfig.addProperty(GraphDatabaseConfiguration.HOSTNAME_KEY, "localhost");

        Configuration accumuloConfig = storageConfig.subset(AccumuloStoreManager.ACCUMULO_CONFIGURATION_NAMESPACE);

        accumuloConfig.addProperty(AccumuloStoreManager.ACCUMULO_INTSANCE_KEY, "MockCloud");

        accumuloConfig.addProperty(AccumuloStoreManager.ACCUMULO_USER_KEY, "root");
        accumuloConfig.addProperty(AccumuloStoreManager.ACCUMULO_PASSWORD_KEY, "");

        return config;
    }

    public static void startAccumulo() throws IOException {
    }

    private static void shutdownAccumulo() throws IOException {
    }
}
