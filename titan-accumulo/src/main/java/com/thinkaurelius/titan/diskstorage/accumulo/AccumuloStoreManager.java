package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Storage Manager for HBase
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class AccumuloStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloStoreManager.class);

    public static final String TABLE_NAME_KEY = "tablename";
    public static final String TABLE_NAME_DEFAULT = "titan";

    public static final int PORT_DEFAULT = 9160;

    public static final String ACCUMULO_CONFIGURATION_NAMESPACE = "accumulo-config";

    public static final ImmutableMap<String, String> ACCUMULO_CONFIGURATION;
    
    static {
        ACCUMULO_CONFIGURATION = new ImmutableMap.Builder<String, String>()
                                    .put(GraphDatabaseConfiguration.HOSTNAME_KEY, "accumulo.zookeeper.quorum")
                                    .put(GraphDatabaseConfiguration.PORT_KEY, "accumulo.zookeeper.property.clientPort")
                                    .build();
    }

    private final String tableName;

    private final ConcurrentMap<String, AccumuloKeyColumnValueStore> openStores;

    private final StoreFeatures features;

    public AccumuloStoreManager(org.apache.commons.configuration.Configuration config) throws StorageException {
        super(config, PORT_DEFAULT);

        this.tableName = config.getString(TABLE_NAME_KEY, TABLE_NAME_DEFAULT);

        openStores = new ConcurrentHashMap<String, AccumuloKeyColumnValueStore>();

        // TODO: allowing publicly mutate fields is bad, should be fixed
        features = new StoreFeatures();
        features.supportsScan = true;
        features.supportsBatchMutation = true;
        features.supportsTransactions = false;
        features.supportsConsistentKeyOperations = true;
        features.supportsLocking = false;
        features.isKeyOrdered = false;
        features.isDistributed = true;
        features.hasLocalKeyPartition = false;
    }


    @Override
    public String toString() {
        return "accumulo[" + tableName + "@" + super.toString() + "]";
    }

    @Override
    public void close() {
        openStores.clear();
    }


    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
    }

    @Override
    public KeyColumnValueStore openDatabase(String dbName) throws StorageException {
        return null;
    }

    
    @Override
    public StoreTransaction beginTransaction(ConsistencyLevel level) throws StorageException {
        return new AccumuloTransaction(level);
    }


    /**
     * Deletes the specified table with all its columns.
     * ATTENTION: Invoking this method will delete the table if it exists and therefore causes data loss.
     */
    @Override
    public void clearStorage() throws StorageException {
    }

    @Override
    public String getConfigurationProperty(final String key) throws StorageException {
        return null;
    }

    @Override
    public void setConfigurationProperty(final String key, final String value) throws StorageException {
    }
}
