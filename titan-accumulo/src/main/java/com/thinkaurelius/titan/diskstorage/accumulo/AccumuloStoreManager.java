package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
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
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockAccumulo;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;

/**
 * Storage Manager for HBase
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class AccumuloStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloStoreManager.class);
    // Configuration namespace
    public static final String ACCUMULO_CONFIGURATION_NAMESPACE = "accumulo-config";
    // Default deleter, scanner, writer parameters 
    private static final Authorizations DEFAULT_AUTHORIZATIONS = new Authorizations();
    private static final Long DEFAULT_MAX_MEMORY = 50 * 1024 * 1024l;
    private static final Long DEFAULT_MAX_LATENCY = 2 * 60 * 1000l;
    private static final Long DEFAULT_TIMEOUT = Long.MAX_VALUE;
    private static final Integer DEFAULT_MAX_QUERY_THREADS = 3;
    private static final Integer DEFAULT_MAX_WRITE_THREADS = 3;
    // Configuration keys
    public static final String ACCUMULO_INTSANCE_KEY = "instance";
    public static final String ACCUMULO_ZOOKEEPERS_KEY = "zookeepers";
    public static final String ACCUMULO_USER_KEY = "username";
    public static final String ACCUMULO_PASSWORD_KEY = "password";
    public static final String TABLE_NAME_KEY = "tablename";
    // Configuration defaults
    public static final String TABLE_NAME_DEFAULT = "titan";
    public static final int PORT_DEFAULT = 9160;
    // Instance variables
    private final String tableName;
    private final String instanceName;
    private final String zooKeepers;
    private final String username;
    private final String password;
    private final Instance instance;
    private final Connector connector;
    private final ConcurrentMap<String, AccumuloKeyColumnValueStore> openStores;
    private final StoreFeatures features;

    public AccumuloStoreManager(Configuration config) throws StorageException {
        super(config, PORT_DEFAULT);

        tableName = config.getString(TABLE_NAME_KEY, TABLE_NAME_DEFAULT);

        // Accumulo specific keys
        Configuration accumuloConfig = config.subset(ACCUMULO_CONFIGURATION_NAMESPACE);
        instanceName = accumuloConfig.getString(ACCUMULO_INTSANCE_KEY);
        zooKeepers = accumuloConfig.getString(ACCUMULO_ZOOKEEPERS_KEY);

        username = accumuloConfig.getString(ACCUMULO_USER_KEY);
        password = accumuloConfig.getString(ACCUMULO_PASSWORD_KEY);

        instance = new ZooKeeperInstance("EtCloud", "localhost");
        try {
            connector = instance.getConnector("root", "bobross".getBytes());
        } catch (AccumuloException ex) {
            logger.error(ex.getMessage(), ex);
            throw new PermanentStorageException(ex.getMessage(), ex);
        } catch (AccumuloSecurityException ex) {
            logger.error(ex.getMessage(), ex);
            throw new PermanentStorageException(ex.getMessage(), ex);
        }

        openStores = new ConcurrentHashMap<String, AccumuloKeyColumnValueStore>();

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
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations,
            StoreTransaction txh) throws StorageException {
        final long delTS = System.currentTimeMillis();
        final long putTS = delTS + 1;

        Map<StaticBuffer, MutablePair<Mutation, Mutation>> commandsPerKey = convertToCommands(mutations, putTS, delTS);


        List<Mutation> batch = new ArrayList<Mutation>(commandsPerKey.size()); // actual batch operation
        // convert sorted commands into representation required for 'batch' operation
        for (MutablePair<Mutation, Mutation> commands : commandsPerKey.values()) {
            if (commands.getFirst() != null) {
                batch.add(commands.getFirst());
            }

            if (commands.getSecond() != null) {
                batch.add(commands.getSecond());
            }
        }

        try {
            BatchWriter writer = connector.createBatchWriter(tableName,
                    DEFAULT_MAX_MEMORY, DEFAULT_MAX_LATENCY, DEFAULT_MAX_WRITE_THREADS);
            try {
                writer.addMutations(batch);
                writer.flush();
            } catch (MutationsRejectedException ex) {
                logger.error(ex.getMessage(), ex);
                throw new PermanentStorageException(ex);
            } finally {
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (MutationsRejectedException ex) {
                        logger.warn(ex.getMessage(), ex);
                    }
                }
            }
        } catch (TableNotFoundException ex) {
            logger.error(ex.getMessage(), ex);
            throw new PermanentStorageException(ex);
        }

        waitUntil(putTS);
    }

    /**
     * Convert Titan internal Mutation representation into Accumulo native
     * commands.
     *
     * @param mutations Mutations to convert into Accumulo commands.
     * @param putTimestamp The timestamp to use for Put mutations.
     * @param delTimestamp The timestamp to use for Delete mutations.
     *
     * @return Commands sorted by key converted from Titan internal
     * representation.
     */
    private static Map<StaticBuffer, MutablePair<Mutation, Mutation>> convertToCommands(Map<String, Map<StaticBuffer, KCVMutation>> mutations,
            final long putTimestamp, final long delTimestamp) {
        Map<StaticBuffer, MutablePair<Mutation, Mutation>> commandsPerKey =
                new HashMap<StaticBuffer, MutablePair<Mutation, Mutation>>();

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> entry : mutations.entrySet()) {
            Text cfName = new Text(entry.getKey().getBytes());

            for (Map.Entry<StaticBuffer, KCVMutation> m : entry.getValue().entrySet()) {
                StaticBuffer key = m.getKey();
                KCVMutation mutation = m.getValue();

                MutablePair<Mutation, Mutation> commands = commandsPerKey.get(key);

                if (commands == null) {
                    commands = new MutablePair<Mutation, Mutation>(null, null);
                    commandsPerKey.put(key, commands);
                }

                if (mutation.hasDeletions()) {
                    if (commands.getSecond() == null) {
                        commands.setSecond(new Mutation(new Text(key.as(StaticBuffer.ARRAY_FACTORY))));
                    }

                    for (StaticBuffer b : mutation.getDeletions()) {
                        commands.getSecond().putDelete(cfName, new Text(b.as(StaticBuffer.ARRAY_FACTORY)), delTimestamp);
                    }
                }

                if (mutation.hasAdditions()) {
                    if (commands.getFirst() == null) {
                        commands.setFirst(new Mutation(new Text(key.as(StaticBuffer.ARRAY_FACTORY))));
                    }

                    for (Entry e : mutation.getAdditions()) {
                        commands.getFirst().put(cfName, new Text(e.getArrayColumn()),
                                putTimestamp, new Value(e.getArrayValue()));
                    }
                }
            }
        }

        return commandsPerKey;
    }

    @Override
    public KeyColumnValueStore openDatabase(String dbName) throws StorageException {
        AccumuloKeyColumnValueStore store = openStores.get(dbName);

        if (store == null) {
            AccumuloKeyColumnValueStore newStore = new AccumuloKeyColumnValueStore(connector, tableName, dbName);

            store = openStores.putIfAbsent(dbName, newStore); // nothing bad happens if we loose to other thread

            if (store == null) { // ensure that CF exists only first time somebody tries to open it
                ensureColumnFamilyExists(tableName, dbName);
                store = newStore;
            }
        }

        return store;
    }

    private void ensureTableExists(String tableName) throws StorageException {
        TableOperations operations = connector.tableOperations();
        if (!operations.exists(tableName)) {
            try {
                operations.create(tableName);
            } catch (AccumuloException ex) {
                logger.error(ex.getMessage(), ex);
                throw new PermanentStorageException(ex);
            } catch (AccumuloSecurityException ex) {
                logger.error(ex.getMessage(), ex);
                throw new PermanentStorageException(ex);
            } catch (TableExistsException ex) {
                logger.warn("Should never throw this exception!", ex);
            }
        }
    }

    private void ensureColumnFamilyExists(String tableName, String columnFamily) throws StorageException {
        ensureTableExists(tableName);
    }

    @Override
    public StoreTransaction beginTransaction(ConsistencyLevel level) throws StorageException {
        return new AccumuloTransaction(level);
    }

    /**
     * Deletes the specified table with all its columns. ATTENTION: Invoking
     * this method will delete the table if it exists and therefore causes data
     * loss.
     */
    @Override
    public void clearStorage() throws StorageException {
        TableOperations operations = connector.tableOperations();

        // first of all, check if table exists, if not - we are done
        if (!operations.exists(tableName)) {
            logger.debug("clearStorage() called before table {} was created, skipping.", tableName);
            return;
        }
        try {
            BatchDeleter deleter = connector.createBatchDeleter(tableName, DEFAULT_AUTHORIZATIONS,
                    DEFAULT_MAX_QUERY_THREADS, DEFAULT_MAX_MEMORY, DEFAULT_MAX_LATENCY, DEFAULT_MAX_WRITE_THREADS);
            deleter.setRanges(Collections.singletonList(new Range()));
            try {
                deleter.delete();
            } catch (MutationsRejectedException ex) {
                logger.error(ex.getMessage(), ex);
                throw new PermanentStorageException(ex);
            } finally {
                deleter.close();
            }

        } catch (TableNotFoundException ex) {
            logger.error(ex.getMessage(), ex);
            throw new PermanentStorageException(ex);
        }
    }

    @Override
    public String getConfigurationProperty(final String key) throws StorageException {
        ensureTableExists(tableName);

        TableOperations operations = connector.tableOperations();
        try {
            Iterable<Map.Entry<String, String>> propIterator = operations.getProperties(tableName);

            Map<String, String> properties = new HashMap<String, String>();
            for (Map.Entry<String, String> entry : propIterator) {
                properties.put(entry.getKey(), entry.getValue());
            }
            return properties.get(key);
        } catch (AccumuloException ex) {
            logger.error(ex.getMessage(), ex);
            throw new PermanentStorageException(ex);
        } catch (TableNotFoundException ex) {
            logger.error(ex.getMessage(), ex);
            throw new PermanentStorageException(ex);
        }
    }

    @Override
    public void setConfigurationProperty(final String key, final String value) throws StorageException {
        ensureTableExists(tableName);
        /*

         TableOperations operations = connector.tableOperations();
         try {
         operations.setProperty(tableName, key, value);
         } catch (AccumuloException ex) {
         logger.error(ex.getMessage(), ex);
         throw new PermanentStorageException(ex);
         } catch (AccumuloSecurityException ex) {
         logger.error(ex.getMessage(), ex);
         throw new PermanentStorageException(ex);
         }
         */
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
