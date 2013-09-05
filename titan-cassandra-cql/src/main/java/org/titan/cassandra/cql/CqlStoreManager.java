package org.titan.cassandra.cql;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

/**
 * CqlStorageManager
 * 
 * @author gciuloaica
 * 
 */
public class CqlStoreManager extends DistributedStoreManager implements
		KeyColumnValueStoreManager {

	private static final Logger log = LoggerFactory
			.getLogger(CqlStoreManager.class);

	/**
	 * Default port at which to attempt Cassandra Native connection.
	 * <p/>
	 * Value = {@value}
	 */
	public static final int PORT_DEFAULT = 9042;

	public static final int REPLICATION_FACTOR_DEFAULT = 1;

	public static final String READ_CONSISTENCY_LEVEL_KEY = "read-consistency-level";
	public static final String READ_CONSISTENCY_LEVEL_DEFAULT = "QUORUM";

	public static final String WRITE_CONSISTENCY_LEVEL_KEY = "write-consistency-level";
	public static final String REPLICATION_FACTOR_KEY = "replication-factor";

	public static final String WRITE_CONSISTENCY_LEVEL_DEFAULT = "QUORUM";

	protected static final String SYSTEM_PROPERTIES_CF = "system_properties";
	protected static final String SYSTEM_PROPERTIES_KEY = "general";

	private final String SYSTEM_INSERT_STMT = "INSERT INTO "
			+ SYSTEM_PROPERTIES_CF
			+ " (rowKey, columnName, value) VALUES (?,?,?)";

	/**
	 * Default name for the Cassandra keyspace
	 * <p/>
	 * Value = {@value}
	 */
	public static final String KEYSPACE_DEFAULT = "titan";
	public static final String KEYSPACE_KEY = "keyspace";

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
	public static final int MAX_CONNECTIONS_PER_HOST_DEFAULT = 6;
	public static final String MAX_CONNECTIONS_PER_HOST_KEY = "max-connections-per-host";

	protected final String keySpaceName;
	protected final int replicationFactor;

	private final int maxConnectionsPerHost;

	private final Consistency readConsistencyLevel;
	private final Consistency writeConsistencyLevel;

	private StoreFeatures features = null;

	private Cluster cluster;

	private final Map<String, CqlKeyColumnValueStore> openStores = new ConcurrentHashMap<String, CqlKeyColumnValueStore>(
			8);

	public CqlStoreManager(Configuration storageConfig) {
		super(storageConfig, PORT_DEFAULT);

		this.keySpaceName = storageConfig.getString(KEYSPACE_KEY,
				KEYSPACE_DEFAULT);

		this.replicationFactor = storageConfig.getInt(REPLICATION_FACTOR_KEY,
				REPLICATION_FACTOR_DEFAULT);

		this.readConsistencyLevel = Consistency.parse(storageConfig.getString(
				READ_CONSISTENCY_LEVEL_KEY, READ_CONSISTENCY_LEVEL_DEFAULT));

		this.writeConsistencyLevel = Consistency.parse(storageConfig.getString(
				WRITE_CONSISTENCY_LEVEL_KEY, WRITE_CONSISTENCY_LEVEL_DEFAULT));

		this.maxConnectionsPerHost = storageConfig.getInt(
				MAX_CONNECTIONS_PER_HOST_KEY, MAX_CONNECTIONS_PER_HOST_DEFAULT);
		connect();
		if (cluster.getMetadata().getKeyspace(keySpaceName) == null) {
			createKeyspace(keySpaceName);
			createSystemTable();
		}

	}

	@Override
	public StoreTransaction beginTransaction(ConsistencyLevel level)
			throws StorageException {
		return new CqlTransaction(level, readConsistencyLevel,
				writeConsistencyLevel);
	}

	@Override
	public void close() throws StorageException {
		log.info("Close Storage.");
		openStores.clear();
		cluster.shutdown();

	}

	@Override
	public void clearStorage() throws StorageException {
		log.info("Clear storage.");
		Session session = cluster.connect();
		session.execute("DROP KEYSPACE " + keySpaceName + ";");
		session.shutdown();
	}

	@Override
	public StoreFeatures getFeatures() {
		if (features == null) {
			features = new StoreFeatures();
			features.supportsScan = true;
			features.supportsBatchMutation = false;
			features.supportsTransactions = false;
			features.supportsConsistentKeyOperations = true;
			features.supportsLocking = false;
			features.isDistributed = true;
			features.isKeyOrdered = false;
			features.hasLocalKeyPartition = false;
		}
		return features;
	}

	@Override
	public String getConfigurationProperty(String key) throws StorageException {
		Session session = cluster.connect(keySpaceName);
		StringBuilder sb = new StringBuilder("SELECT value FROM ")
				.append(SYSTEM_PROPERTIES_CF).append(" WHERE rowKey=? ")
				.append(" AND columnName=?");

		PreparedStatement stmt = session.prepare(sb.toString());
		BoundStatement boundStmt = new BoundStatement(stmt);
		boundStmt.setConsistencyLevel(Consistency.QUORUM.getCqlConsistency());
		boundStmt.setBytes("rowKey",
				ByteBuffer.wrap(SYSTEM_PROPERTIES_KEY.getBytes()));
		boundStmt.setString("columnName", key);
		ResultSet rs = session.execute(boundStmt);
		String value = null;
		if (rs.iterator().hasNext()) {
			Row row = rs.iterator().next();
			ByteBuffer bb = row.getBytes("value");
			int length = bb.remaining();
			byte[] buffer = new byte[length];
			bb.get(buffer, 0, length);
			value = new String(buffer);
		}
		session.shutdown();
		return value;

	}

	@Override
	public void setConfigurationProperty(String key, String value)
			throws StorageException {
		Session session = cluster.connect(keySpaceName);
		PreparedStatement stmt = session.prepare(SYSTEM_INSERT_STMT);
		BoundStatement boundStmt = new BoundStatement(stmt);
		boundStmt.setConsistencyLevel(Consistency.QUORUM.getCqlConsistency());
		boundStmt.setBytes("rowKey",
				ByteBuffer.wrap(SYSTEM_PROPERTIES_KEY.getBytes()));
		boundStmt.setString("columnName", key);
		boundStmt.setBytes("value", ByteBuffer.wrap(value.getBytes()));
		session.execute(boundStmt);
		session.shutdown();

	}

	@Override
	public CqlKeyColumnValueStore openDatabase(String name)
			throws StorageException {
		if (openStores.containsKey(name)) {
			return openStores.get(name);
		} else {
			ensureColumnFamilyExists(name);
			Session session = cluster.connect(keySpaceName);
			CqlKeyColumnValueStore store = new CqlKeyColumnValueStore(name,
					session);
			openStores.put(name, store);
			return store;
		}

	}

	@Override
	public void mutateMany(
			Map<String, Map<StaticBuffer, KCVMutation>> mutations,
			StoreTransaction txh) throws StorageException {
		// TODO Auto-generated method stub
		log.info("Mutate many");

	}

	private void connect() {
		Cluster.Builder builder = Cluster.builder().addContactPoints(
				StringUtils.join(hostnames, ","));
		builder.poolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL,
				maxConnectionsPerHost);
		cluster = builder.build();
		Metadata metadata = cluster.getMetadata();
		log.info("Connected to cluster: %s\n", metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			log.info("Datatacenter: {}; Host: {}; Rack: {}\n",
					new Object[] { host.getDatacenter(),
							host.getAddress().toString(), host.getRack() });
		}
	}

	private void createKeyspace(String name) {
		Session session = cluster.connect();
		StringBuilder sb = new StringBuilder("CREATE KEYSPACE ")
				.append(name)
				.append(" WITH replication = {'class':'SimpleStrategy', 'replication_factor':")
				.append(String.valueOf(replicationFactor)).append("};");
		session.execute(sb.toString());
		session.shutdown();
	}

	private void ensureColumnFamilyExists(String name) {
		Metadata metadata = cluster.getMetadata();
		KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keySpaceName);
		Collection<TableMetadata> tablesMetadata = keyspaceMetadata.getTables();
		boolean isTablePresent = false;
		for (TableMetadata tableMetdata : tablesMetadata) {
			if (tableMetdata.getName().equalsIgnoreCase(name)) {
				isTablePresent = true;
				break;
			}
		}
		if (!isTablePresent) {
			createTable(name);
		}

	}

	private void createTable(String name) {
		Session session = cluster.connect(keySpaceName);
		StringBuilder sb = new StringBuilder("CREATE TABLE ")
				.append(name)
				.append(" (rowKey blob, columnName blob, value blob, PRIMARY KEY(rowKey, columnName));");
		session.execute(sb.toString());
		session.shutdown();

	}

	private void createSystemTable() {
		Session session = cluster.connect(keySpaceName);
		StringBuilder sb = new StringBuilder("CREATE TABLE ")
				.append(SYSTEM_PROPERTIES_CF)
				.append(" (rowKey blob, columnName text, value blob, PRIMARY KEY(rowKey, columnName));");
		session.execute(sb.toString());
		session.shutdown();

	}

	@Override
	public String getName() {
		return KEYSPACE_DEFAULT;
	}

}
