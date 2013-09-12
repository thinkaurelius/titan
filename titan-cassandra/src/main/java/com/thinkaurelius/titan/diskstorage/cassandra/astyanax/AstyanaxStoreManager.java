package com.thinkaurelius.titan.diskstorage.cassandra.astyanax;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.*;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.FixedRetryBackoffStrategy;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction;
import com.thinkaurelius.titan.diskstorage.cassandra.astyanax.locking.AstyanaxRecipeLocker;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;

public class AstyanaxStoreManager extends AbstractCassandraStoreManager {

    private static final Logger log = LoggerFactory.getLogger(AstyanaxStoreManager.class);

    //################### ASTYANAX SPECIFIC CONFIGURATION OPTIONS ######################

    /**
     * Default name for the Cassandra cluster
     * <p/>
     * Value = {@value}
     */
    public static final String CLUSTER_DEFAULT = "Titan Cluster";
    public static final String CLUSTER_KEY = "cluster-name";

    /**
     * Maximum pooled connections per host.
     * <p/>
     * Value = {@value}
     */
    public static final int MAX_CONNECTIONS_PER_HOST_DEFAULT = 32;
    public static final String MAX_CONNECTIONS_PER_HOST_KEY = "max-connections-per-host";
    
    /**
     * Maximum open connections allowed in the pool (counting all hosts).
     * <p/>
     * Value = {@value}
     */
    public static final int MAX_CONNECTIONS_DEFAULT = -1;
    public static final String MAX_CONNECTIONS_KEY = "max-connections";
    
    /**
     * Maximum number of operations allowed per connection before the connection is closed.
     * <p/>
     * Value = {@value}
     */
    public static final int MAX_OPERATIONS_PER_CONNECTION_DEFAULT = 100 * 1000;
    public static final String MAX_OPERATIONS_PER_CONNECTION_KEY = "max-operations-per-connection";

    /**
     * Maximum pooled "cluster" connections per host.
     * <p/>
     * These connections are mostly idle and only used for DDL operations
     * (like creating keyspaces).  Titan doesn't need many of these connections
     * in ordinary operation.
     */
    public static final int MAX_CLUSTER_CONNECTIONS_PER_HOST_DEFAULT = 3;
    public static final String MAX_CLUSTER_CONNECTIONS_PER_HOST_KEY = "max-cluster-connections-per-host";

    /**
     * How Astyanax discovers Cassandra cluster nodes. This must be one of the
     * values of the Astyanax NodeDiscoveryType enum.
     * <p/>
     * Value = {@value}
     */
    public static final String NODE_DISCOVERY_TYPE_DEFAULT = "RING_DESCRIBE";
    public static final String NODE_DISCOVERY_TYPE_KEY = "node-discovery-type";

    /**
     * Astyanax's connection pooler implementation. This must be one of the
     * values of the Astyanax ConnectionPoolType enum.
     * <p/>
     * Value = {@value}
     */
    public static final String CONNECTION_POOL_TYPE_DEFAULT = "TOKEN_AWARE";
    public static final String CONNECTION_POOL_TYPE_KEY = "connection-pool-type";

    /**
     * This must be the fully-qualified classname (i.e. the complete package
     * name, a dot, and then the class name) of an implementation of Astyanax's
     * RetryPolicy interface. This string may be followed by a sequence of
     * integers, separated from the full classname and from each other by
     * commas; in this case, the integers are cast to native Java ints and
     * passed to the class constructor as arguments.
     * <p/>
     * Value = {@value}
     */
    public static final String RETRY_POLICY_DEFAULT = "com.netflix.astyanax.retry.BoundedExponentialBackoff,100,25000,8";
    public static final String RETRY_POLICY_KEY = "retry-policy";

    private static final ColumnFamily<String, String> PROPERTIES_CF;

    static {
        PROPERTIES_CF = new ColumnFamily<String, String>(SYSTEM_PROPERTIES_CF,
                                                         StringSerializer.get(),
                                                         StringSerializer.get(),
                                                         StringSerializer.get());
    }

    private final String clusterName;

    private final AstyanaxContext<Keyspace> keyspaceContext;
    private final AstyanaxContext<Cluster> clusterContext;

    private final RetryPolicy retryPolicy;

    private final Map<String, AstyanaxOrderedKeyColumnValueStore> openStores;

    public AstyanaxStoreManager(Configuration config) throws StorageException {
        super(config);

        // Check if we have non-default thrift frame size or max message size set and warn users
        // because there is nothing we can do in Astyanax to apply those, warning is good enough here
        // otherwise it would make bad user experience if we don't warn at all or crash on this.
        if (this.thriftFrameSize != THRIFT_DEFAULT_FRAME_SIZE)
            log.warn("Couldn't set custom Thrift Frame Size property, use 'cassandrathrift' instead.");

        this.clusterName = config.getString(CLUSTER_KEY, CLUSTER_DEFAULT);

        this.retryPolicy = getRetryPolicy(config.getString(RETRY_POLICY_KEY, RETRY_POLICY_DEFAULT));

        final int maxConnsPerHost =
                config.getInt(
                        MAX_CONNECTIONS_PER_HOST_KEY,
                        MAX_CONNECTIONS_PER_HOST_DEFAULT);

        final int maxClusterConnsPerHost =
                config.getInt(
                        MAX_CLUSTER_CONNECTIONS_PER_HOST_KEY,
                        MAX_CLUSTER_CONNECTIONS_PER_HOST_DEFAULT);

        this.clusterContext = createCluster(getContextBuilder(config, maxClusterConnsPerHost, "Cluster"));

        ensureKeyspaceExists(clusterContext.getClient());

        this.keyspaceContext = getContextBuilder(config, maxConnsPerHost, "Keyspace").buildKeyspace(ThriftFamilyFactory.getInstance());
        this.keyspaceContext.start();

        openStores = new HashMap<String, AstyanaxOrderedKeyColumnValueStore>(8);
    }

    @Override
    public Partitioner getPartitioner() throws StorageException {
        Cluster cl = clusterContext.getClient();
        try {
            return Partitioner.getPartitioner(cl.describePartitioner());
        } catch (ConnectionException e) {
            throw new TemporaryStorageException(e);
        }
    }

    @Override
    public String toString() {
        return "astyanax" + super.toString();
    }

    @Override
    public void close() {
        // Shutdown the Astyanax contexts
        openStores.clear();
        keyspaceContext.shutdown();
        clusterContext.shutdown();
    }

    @Override
    public synchronized AstyanaxOrderedKeyColumnValueStore openDatabase(String name) throws StorageException {
        if (openStores.containsKey(name)) return openStores.get(name);
        else {
            ensureColumnFamilyExists(name);
            AstyanaxOrderedKeyColumnValueStore store = new AstyanaxOrderedKeyColumnValueStore(name, keyspaceContext.getClient(), this, retryPolicy);
            openStores.put(name, store);
            return store;
        }
    }
    
    public AstyanaxRecipeLocker openLocker(String cfName) throws StorageException {
        
        ColumnFamily<ByteBuffer, String> cf = new ColumnFamily<ByteBuffer, String>(
                cfName,
                ByteBufferSerializer.get(),
                StringSerializer.get());
        
        return new AstyanaxRecipeLocker.Builder(keyspaceContext.getClient(), cf).build();
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> batch, StoreTransaction txh) throws StorageException {
        CassandraTransaction ctxh = getTx(txh);
        MutationBatch m = keyspaceContext.getClient().prepareMutationBatch()
                                                     .setConsistencyLevel(getTx(txh).getWriteConsistencyLevel().getAstyanaxConsistency())
                                                     .withRetryPolicy(retryPolicy.duplicate());

        final long delTS;
        final long addTS;

        if (ctxh.getTimestamp() == null) {
            // If cassandra transaction timestamp not provided, use current time
            delTS = TimeUtility.INSTANCE.getApproxNSSinceEpoch(false);
            addTS = TimeUtility.INSTANCE.getApproxNSSinceEpoch(true);
        } else {
            // Set the transaction timestamp according to transaction. Deletions get
            // the earlier timestamp.
            delTS = ctxh.getTimestamp();
            addTS = ctxh.getTimestamp() + 1;
        }

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> batchentry : batch.entrySet()) {
            String storeName = batchentry.getKey();
            Preconditions.checkArgument(openStores.containsKey(storeName), "Store cannot be found: " + storeName);

            ColumnFamily<ByteBuffer, ByteBuffer> columnFamily = openStores.get(storeName).getColumnFamily();

            Map<StaticBuffer, KCVMutation> mutations = batchentry.getValue();
            for (Map.Entry<StaticBuffer, KCVMutation> ent : mutations.entrySet()) {
                // The CLMs for additions and deletions are separated because
                // Astyanax's operation timestamp cannot be set on a per-delete
                // or per-addition basis.
                KCVMutation titanMutation = ent.getValue();

                if (titanMutation.hasDeletions()) {
                    ColumnListMutation<ByteBuffer> dels = m.withRow(columnFamily, ent.getKey().asByteBuffer());
                    dels.setTimestamp(delTS);

                    for (StaticBuffer b : titanMutation.getDeletions())
                        dels.deleteColumn(b.asByteBuffer());
                }

                if (titanMutation.hasAdditions()) {
                    ColumnListMutation<ByteBuffer> upds = m.withRow(columnFamily, ent.getKey().asByteBuffer());
                    upds.setTimestamp(addTS);

                    for (Entry e : titanMutation.getAdditions())
                        upds.putColumn(e.getColumn().asByteBuffer(), e.getValue().asByteBuffer());
                }
            }
        }

        try {
            m.execute();
        } catch (ConnectionException e) {
            throw new TemporaryStorageException(e);
        }
    }

    @Override
    public void clearStorage() throws StorageException {
        try {
            Cluster cluster = clusterContext.getClient();

            Keyspace ks = cluster.getKeyspace(keySpaceName);

            // Not a big deal if Keyspace doesn't not exist (dropped manually by user or tests).
            // This is called on per test setup basis to make sure that previous test cleaned
            // everything up, so first invocation would always fail as Keyspace doesn't yet exist.
            if (ks == null)
                return;

            for (ColumnFamilyDefinition cf : cluster.describeKeyspace(keySpaceName).getColumnFamilyList()) {
                ks.truncateColumnFamily(new ColumnFamily<Object, Object>(cf.getName(), null, null));
            }
        } catch (ConnectionException e) {
            throw new PermanentStorageException(e);
        }
    }

    private void ensureColumnFamilyExists(String name) throws StorageException {
        ensureColumnFamilyExists(name, "org.apache.cassandra.db.marshal.BytesType");
    }

    private void ensureColumnFamilyExists(String name, String comparator) throws StorageException {
        Cluster cl = clusterContext.getClient();
        try {
            KeyspaceDefinition ksDef = cl.describeKeyspace(keySpaceName);
            boolean found = false;
            if (null != ksDef) {
                for (ColumnFamilyDefinition cfDef : ksDef.getColumnFamilyList()) {
                    found |= cfDef.getName().equals(name);
                }
            }
            if (!found) {
                ColumnFamilyDefinition cfDef =
                        cl.makeColumnFamilyDefinition()
                                .setName(name)
                                .setKeyspace(keySpaceName)
                                .setComparatorType(comparator)
                                .setCompressionOptions(
                                		new ImmutableMap.Builder<String, String>()
                                			.put("sstable_compression", "SnappyCompressor")
                                			.put("chunk_length_kb", "64")
                                			.build()
                                );
                cl.addColumnFamily(cfDef);
            }
        } catch (ConnectionException e) {
            throw new TemporaryStorageException(e);
        }
    }

    private static AstyanaxContext<Cluster> createCluster(AstyanaxContext.Builder cb) {
        AstyanaxContext<Cluster> clusterCtx = cb.buildCluster(ThriftFamilyFactory.getInstance());
        clusterCtx.start();

        return clusterCtx;
    }

    private AstyanaxContext.Builder getContextBuilder(Configuration config, int maxConnsPerHost, String usedFor) {

        final ConnectionPoolType poolType = ConnectionPoolType.valueOf(
                config.getString(
                        CONNECTION_POOL_TYPE_KEY,
                        CONNECTION_POOL_TYPE_DEFAULT));

        final NodeDiscoveryType discType = NodeDiscoveryType.valueOf(
                config.getString(
                        NODE_DISCOVERY_TYPE_KEY,
                        NODE_DISCOVERY_TYPE_DEFAULT));
        
        final int maxConnections =
                config.getInt(
                        MAX_CONNECTIONS_KEY,
                        MAX_CONNECTIONS_DEFAULT);
        
        final int maxOperationsPerConnection =
        		config.getInt(
                        MAX_OPERATIONS_PER_CONNECTION_KEY,
                        MAX_OPERATIONS_PER_CONNECTION_DEFAULT);
        
        ConnectionPoolConfigurationImpl cpool =
        		new ConnectionPoolConfigurationImpl(usedFor + "TitanConnectionPool")
        			.setPort(port)
        			.setMaxOperationsPerConnection(maxOperationsPerConnection)
        			.setMaxConnsPerHost(maxConnsPerHost)
        			.setRetryBackoffStrategy(new FixedRetryBackoffStrategy(1000, 5000)) // TODO configuration
        			.setSocketTimeout(connectionTimeout)
        			.setConnectTimeout(connectionTimeout)
        			.setSeeds(StringUtils.join(hostnames,","));
        
        AstyanaxConfigurationImpl aconf =
                new AstyanaxConfigurationImpl()
                    .setConnectionPoolType(poolType)
                    .setDiscoveryType(discType)
                    .setTargetCassandraVersion("1.2");
        
        if (0 < maxConnections) {
            cpool.setMaxConns(maxConnections);
        }

        AstyanaxContext.Builder builder =
                new AstyanaxContext.Builder()
                        .forCluster(clusterName)
                        .forKeyspace(keySpaceName)
                        .withAstyanaxConfiguration(aconf)
                        .withConnectionPoolConfiguration(cpool)
                        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor());

        return builder;
    }

    private void ensureKeyspaceExists(Cluster cl) throws StorageException {
        KeyspaceDefinition ksDef;

        try {
            ksDef = cl.describeKeyspace(keySpaceName);

            if (null != ksDef && ksDef.getName().equals(keySpaceName)) {
                log.debug("Found keyspace {}", keySpaceName);
                return;
            }
        } catch (ConnectionException e) {
            log.debug("Failed to describe keyspace {}", keySpaceName);
        }

        log.debug("Creating keyspace {}...", keySpaceName);
        try {
            Map<String, String> stratops = new HashMap<String, String>() {{
                put("replication_factor", String.valueOf(replicationFactor));
            }};

            ksDef = cl.makeKeyspaceDefinition()
                    .setName(keySpaceName)
                    .setStrategyClass("org.apache.cassandra.locator.SimpleStrategy")
                    .setStrategyOptions(stratops);
            cl.addKeyspace(ksDef);

            log.debug("Created keyspace {}", keySpaceName);
        } catch (ConnectionException e) {
            log.debug("Failed to create keyspace {}, keySpaceName");
            throw new TemporaryStorageException(e);
        }
    }

    private static RetryPolicy getRetryPolicy(String serializedRetryPolicy) throws StorageException {
        String[] tokens = serializedRetryPolicy.split(",");
        String policyClassName = tokens[0];
        int argCount = tokens.length - 1;
        Object[] args = new Object[argCount];
        for (int i = 1; i < tokens.length; i++) {
            args[i - 1] = Integer.valueOf(tokens[i]);
        }

        try {
            RetryPolicy rp = instantiateRetryPolicy(policyClassName, args, serializedRetryPolicy);
            log.debug("Instantiated RetryPolicy object {} from config string \"{}\"", rp, serializedRetryPolicy);
            return rp;
        } catch (Exception e) {
            throw new PermanentStorageException("Failed to instantiate Astyanax Retry Policy class", e);
        }
    }

    private static RetryPolicy instantiateRetryPolicy(String policyClassName,
                                                      Object[] args, String raw) throws Exception {

        Class<?> policyClass = Class.forName(policyClassName);

        for (Constructor<?> con : policyClass.getConstructors()) {
            Class<?>[] parameterClasses = con.getParameterTypes();
            if (args.length == parameterClasses.length) {
                boolean allInts = true;
                for (Class<?> pc : parameterClasses) {
                    if (!pc.equals(int.class)) {
                        allInts = false;
                        break;
                    }
                }

                if (!allInts) {
                    break;
                }

                log.debug("About to instantiate class {} with {} arguments", con.toString(), args.length);

                return (RetryPolicy) con.newInstance(args);
            }
        }

        throw new Exception("Failed to identify a class matching the Astyanax Retry Policy config string \"" + raw + "\"");
    }

    @Override
    public String getConfigurationProperty(final String key) throws StorageException {
        try {
            ensureColumnFamilyExists(SYSTEM_PROPERTIES_CF, "org.apache.cassandra.db.marshal.UTF8Type");

            OperationResult<Column<String>> result =
                    keyspaceContext.getClient().prepareQuery(PROPERTIES_CF)
                                               .setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
                                               .withRetryPolicy(retryPolicy.duplicate())
                                               .getKey(SYSTEM_PROPERTIES_KEY).getColumn(key)
                                               .execute();

            return result.getResult().getStringValue();
        } catch (NotFoundException e) {
                return null;
        } catch (ConnectionException e) {
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public void setConfigurationProperty(final String key, final String value) throws StorageException {
        try {
            ensureColumnFamilyExists(SYSTEM_PROPERTIES_CF, "org.apache.cassandra.db.marshal.UTF8Type");

            Keyspace ks = keyspaceContext.getClient();

            OperationResult<Void> result = ks.prepareColumnMutation(PROPERTIES_CF, SYSTEM_PROPERTIES_KEY, key)
                                                    .setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
                                                    .putValue(value, null)
                                                    .execute();

            result.getResult();
        } catch (ConnectionException e) {
            throw new PermanentStorageException(e);
        }
    }
    
    @Override
    public Map<String, String> getCompressionOptions(String cf) throws StorageException {
        try {
            Keyspace k = keyspaceContext.getClient();
        
            KeyspaceDefinition kdef = k.describeKeyspace();
        
            if (null == kdef) {
                throw new PermanentStorageException("Keyspace " + kdef + " is undefined");
            }
        
            ColumnFamilyDefinition cfdef = kdef.getColumnFamily(cf);
        
            if (null == cfdef) {
                throw new PermanentStorageException("Column family " + cf + " is undefined");
            }
        
            return cfdef.getCompressionOptions();
        } catch (ConnectionException e) {
            throw new PermanentStorageException(e);
        }
    }
}


