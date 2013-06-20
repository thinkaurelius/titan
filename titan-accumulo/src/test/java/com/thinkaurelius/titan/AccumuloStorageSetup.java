package com.thinkaurelius.titan;

import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;

public class AccumuloStorageSetup {

    private static Process ACCUMULO = null;
    // amount of seconds to wait before assuming that HBase shutdown
    private static final int SHUTDOWN_TIMEOUT_SEC = 20;
    // hbase config for testing
    private static final String ACCUMULO_CONFIG_DIR = "./src/test/config";
    // default pid file location
    private static final String ACCUMULO_PID_FILE = "/tmp/accumulo-" + System.getProperty("user.name") + "-master.pid";

    static {
        try {
            System.out.println("Deleteing old test directories (if any).");

            // please keep in sync with HBASE_CONFIG_DIR/hbase-site.xml, reading HBase XML config is huge pain.
            File accumuloRoot = new File("./src/test/titan-accumulo-test-data");
            File zookeeperDataDir = new File("./src/test/titan-zookeeper-test");

            if (accumuloRoot.exists()) {
                FileUtils.deleteDirectory(accumuloRoot);
            }

            if (zookeeperDataDir.exists()) {
                FileUtils.deleteDirectory(zookeeperDataDir);
            }
        } catch (IOException e) {
            System.err.println("Failed to delete old Accumulo test directories: '" + e.getMessage() + "', ignoring.");
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("All done. Shutting done Accumulo.");

                try {
                    AccumuloStorageSetup.shutdownAccumulo();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

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

        accumuloConfig.addProperty(AccumuloStoreManager.ACCUMULO_INTSANCE_KEY, "EtCloud");

        accumuloConfig.addProperty(AccumuloStoreManager.ACCUMULO_USER_KEY, "root");
        accumuloConfig.addProperty(AccumuloStoreManager.ACCUMULO_PASSWORD_KEY, "");

        return config;
    }

    public static void startAccumulo() throws IOException {
    }

    private static void shutdownAccumulo() throws IOException {
    }
}
