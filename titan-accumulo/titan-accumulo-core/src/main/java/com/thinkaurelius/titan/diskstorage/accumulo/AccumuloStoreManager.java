package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storage Manager for Accumulo.
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public class AccumuloStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloStoreManager.class);
    // Default parameters 
    private static final Authorizations AUTHORIZATIONS_DEFAULT = new Authorizations();
    // Configuration namespace
    public static final String ACCUMULO_CONFIGURATION_NAMESPACE = "accumulo-config";
    // Configuration keys
    public static final String ACCUMULO_INTSANCE_KEY = "instance";
    public static final String TABLE_NAME_KEY = "tablename";
    public static final String SERVER_SIDE_ITERATORS_KEY = "server-side-iterators";
    // Configuration defaults
    public static final String TABLE_NAME_DEFAULT = "titan";
    public static final int PORT_DEFAULT = 9160;
    public static final boolean SERVER_SIDE_ITERATORS_DEFAULT = false;
    // Instance injector
    public static AccumuloInstanceFactory instanceFactory = AccumuloInstanceFactory.ZOOKEEPER_INSTANCE_FACTORY;
    // Instance variables
    private final String tableName;
    private final String instanceName;
    private final String zooKeepers;
    private final boolean serverSideIterators;
    private final Instance instance;    // thread-safe
    private final Connector connector;  // thread-safe
    private final ConcurrentMap<String, AccumuloKeyColumnValueStore> openStores;
    private final StoreFeatures features;   // immutable at constructor exit
    private final AccumuloBatchConfiguration batchConfiguration;    // immutable at constructor exit
    private final AccumuloStoreConfiguration storeConfiguration;    // immutable at constructor exit

    public AccumuloStoreManager(Configuration config) throws StorageException {
        super(config, PORT_DEFAULT);

        zooKeepers = config.getString(GraphDatabaseConfiguration.HOSTNAME_KEY,
                GraphDatabaseConfiguration.HOSTNAME_DEFAULT);
        tableName = config.getString(TABLE_NAME_KEY, TABLE_NAME_DEFAULT);

        // Accumulo specific keys
        Configuration accumuloConfig = config.subset(ACCUMULO_CONFIGURATION_NAMESPACE);
        instanceName = accumuloConfig.getString(ACCUMULO_INTSANCE_KEY);

        serverSideIterators = accumuloConfig.getBoolean(SERVER_SIDE_ITERATORS_KEY, SERVER_SIDE_ITERATORS_DEFAULT);

        instance = instanceFactory.getInstance(instanceName, zooKeepers);

        try {
            connector = instance.getConnector(username, password.getBytes());
        } catch (AccumuloException ex) {
            logger.error("Accumulo failure", ex);
            throw new PermanentStorageException(ex.getMessage(), ex);
        } catch (AccumuloSecurityException ex) {
            logger.error("User doesn't have permission to connect", ex);
            throw new PermanentStorageException(ex.getMessage(), ex);
        }

        openStores = new ConcurrentHashMap<String, AccumuloKeyColumnValueStore>();

        features = new StoreFeatures();
        features.supportsOrderedScan = true;
        features.supportsUnorderedScan = true;
        features.supportsBatchMutation = true;
        features.supportsTransactions = false;
        features.supportsMultiQuery = true;
        features.supportsConsistentKeyOperations = true;
        features.supportsLocking = false;
        features.isKeyOrdered = false;
        features.isDistributed = true;
        features.hasLocalKeyPartition = false;

        batchConfiguration = new AccumuloBatchConfiguration();

        storeConfiguration = new AccumuloStoreConfiguration(connector, tableName, batchConfiguration);
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public String toString() {
        return "accumulo[" + getName() + "@" + super.toString() + "]";
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
    public KeyColumnValueStore openDatabase(String dbName) throws StorageException {
        AccumuloKeyColumnValueStore store = openStores.get(dbName);

        if (store == null) {
            AccumuloKeyColumnValueStore newStore = new AccumuloKeyColumnValueStore(connector, tableName, dbName,
                    batchConfiguration, serverSideIterators);

            store = openStores.putIfAbsent(dbName, newStore); // atomic so only one store dbName

            if (store == null) { // ensure that column family exists on first open
                ensureColumnFamilyExists(tableName, dbName);
                store = newStore;
            }
        }

        return store;
    }

    @Override
    public void clearStorage() throws StorageException {
        TableOperations operations = connector.tableOperations();

        // Check if table exists, if not we are done
        if (!operations.exists(tableName)) {
            logger.warn("clearStorage() called before table {} created, skipping.", tableName);
            return;
        }

        try {
            BatchDeleter deleter = batchConfiguration.createBatchDeleter(connector, tableName, AUTHORIZATIONS_DEFAULT);
            deleter.setRanges(Collections.singletonList(new Range()));
            try {
                deleter.delete();
            } catch (MutationsRejectedException ex) {
                logger.error("Can't write mutations to " + tableName, ex);
                throw new TemporaryStorageException(ex);
            } finally {
                deleter.close();
            }

        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan table " + tableName, ex);
            throw new PermanentStorageException(ex);
        }
    }

    public String getConfigurationProperty(String key) throws StorageException {
        return storeConfiguration.getConfigurationProperty(key);
    }

    public void setConfigurationProperty(String key, String value) throws StorageException {
        storeConfiguration.setConfigurationProperty(key, value);
    }

    @Override
    public StoreTransaction beginTransaction(final StoreTxConfig config) throws StorageException {
        return new AccumuloTransaction(config);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        final long delTS = System.currentTimeMillis();
        final long putTS = delTS + 1;

        Collection<Mutation> actions = convertToActions(mutations, putTS, delTS);

        try {
            BatchWriter writer = batchConfiguration.createBatchWriter(connector, tableName);
            try {
                writer.addMutations(actions);
                writer.flush();
            } catch (MutationsRejectedException ex) {
                logger.error("Can't write mutations to Titan store " + tableName, ex);
                throw new TemporaryStorageException(ex);
            } finally {
                try {
                    writer.close();
                } catch (MutationsRejectedException ex) {
                    logger.error("Can't write mutations to Titan store " + tableName, ex);
                    throw new TemporaryStorageException(ex);
                }
            }
        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }

        waitUntil(putTS);
    }

    /**
     * Convert Titan internal {
     *
     * @ KCVMutation} representation into Accumulo native {
     * @ Mutation}.
     *
     * @param mutations Mutations to convert into Accumulo actions.
     * @param putTimestamp The timestamp to use for put mutations.
     * @param delTimestamp The timestamp to use for delete mutations.
     *
     * @return Mutations converted from Titan internal representation.
     */
    private static Collection<Mutation> convertToActions(Map<String, Map<StaticBuffer, KCVMutation>> mutations,
            final long putTimestamp, final long delTimestamp) {

        Map<StaticBuffer, Mutation> actionsPerKey = new HashMap<StaticBuffer, Mutation>();

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> entry : mutations.entrySet()) {
            Text colFamily = new Text(entry.getKey().getBytes());

            for (Map.Entry<StaticBuffer, KCVMutation> m : entry.getValue().entrySet()) {
                StaticBuffer key = m.getKey();
                KCVMutation mutation = m.getValue();

                Mutation commands = actionsPerKey.get(key);

                if (commands == null) {
                    commands = new Mutation(new Text(key.as(StaticBuffer.ARRAY_FACTORY)));
                    actionsPerKey.put(key, commands);
                }

                if (mutation.hasDeletions()) {
                    for (StaticBuffer del : mutation.getDeletions()) {
                        commands.putDelete(colFamily, new Text(del.as(StaticBuffer.ARRAY_FACTORY)), delTimestamp);
                    }
                }

                if (mutation.hasAdditions()) {
                    for (Entry add : mutation.getAdditions()) {
                        commands.put(colFamily, new Text(add.getArrayColumn()), putTimestamp, new Value(add.getArrayValue()));
                    }
                }
            }
        }

        return actionsPerKey.values();
    }

    private void ensureColumnFamilyExists(String tableName, String columnFamily) throws StorageException {
        ensureTableExists(tableName);
        // Option to set locality groups here
    }

    private void ensureTableExists(String tableName) throws StorageException {
        TableOperations operations = connector.tableOperations();
        if (!operations.exists(tableName)) {
            try {
                operations.create(tableName);
            } catch (AccumuloException ex) {
                logger.error("Accumulo failure", ex);
                throw new PermanentStorageException(ex);
            } catch (AccumuloSecurityException ex) {
                logger.error("User doesn't have permission to create Titan store" + tableName, ex);
                throw new PermanentStorageException(ex);
            } catch (TableExistsException ex) {
                // Concurrent creation of table, this thread lost race
            }
        }
    }

    private static void waitUntil(long until) {
        long now = System.currentTimeMillis();

        while (now <= until) {
            try {
                Thread.sleep(1L);
                now = System.currentTimeMillis();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}