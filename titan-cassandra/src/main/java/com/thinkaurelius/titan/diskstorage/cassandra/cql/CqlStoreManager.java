package com.thinkaurelius.titan.diskstorage.cassandra.cql;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;

/**
 * CqlStorageManager
 * 
 * @author gciuloaica
 * 
 */
public class CqlStoreManager extends AbstractCassandraStoreManager {
	private static final Logger logger = LoggerFactory.getLogger(CqlStoreManager.class);

	private final int maxConnectionsPerHost;

	private final Cluster cluster;
    private final IPartitioner partitioner;

	private final Map<String, CqlKeyColumnValueStore> openStores = new ConcurrentHashMap<String, CqlKeyColumnValueStore>(
			8);

	public CqlStoreManager(Configuration storageConfig) {
		super(storageConfig);

		maxConnectionsPerHost = storageConfig.get(GraphDatabaseConfiguration.CONNECTION_POOL_SIZE) / hostnames.length;

        cluster = connect();

        if (cluster.getMetadata().getKeyspace(keySpaceName) == null) {
            createKeyspace(keySpaceName);
        }

        // !!! There is no other way but to use Thrift to get partitioner :( */
        TTransport transport = null;

        IPartitioner partitioner;

        try {
            transport = new TFramedTransport(new TSocket(hostnames[0], port, 1000));
            TBinaryProtocol protocol = new TBinaryProtocol(transport);
            Cassandra.Client client = new Cassandra.Client(protocol);
            transport.open();

            partitioner = FBUtilities.newPartitioner(client.describe_partitioner());
        } catch (Exception e) {
            partitioner = new RandomPartitioner();
        } finally {
            if (transport != null)
                transport.close();
        }

        this.partitioner = partitioner;
    }

    @Override
    public IPartitioner<? extends Token<?>> getCassandraPartitioner() throws StorageException {
        return partitioner;
    }

    @Override
    public Deployment getDeployment() {
        return Deployment.REMOTE;
    }

    @Override
	public void close() throws StorageException {
		openStores.clear();
		cluster.close();

	}

	@Override
	public void clearStorage() throws StorageException {
        Session session = cluster.connect();
        try {
            session.execute("DROP KEYSPACE " + keySpaceName + ";");
        } finally {
            session.close();
        }
    }

    @Override
    public Map<String, String> getCompressionOptions(String cf) throws StorageException {
        TableMetadata meta = cluster.getMetadata().getKeyspace(keySpaceName).getTable(cf);
        return meta.getOptions().getCompression();
    }

    @Override
	public CqlKeyColumnValueStore openDatabase(String name) throws StorageException {
		if (openStores.containsKey(name))
			return openStores.get(name);

        ensureColumnFamilyExists(name);
        Session session = cluster.connect(keySpaceName);
        CqlKeyColumnValueStore store = new CqlKeyColumnValueStore(name, session);
        openStores.put(name, store);
        return store;
	}

	@Override
	public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        Preconditions.checkNotNull(mutations);

        // TODO: we should use BatchStatement once we switch to 2.0.x
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> keyMutation : mutations.entrySet()) {
            CqlKeyColumnValueStore store = openDatabase(keyMutation.getKey());

            for (Map.Entry<StaticBuffer, KCVMutation> mutEntry : keyMutation.getValue().entrySet()) {
                StaticBuffer key = mutEntry.getKey();
                KCVMutation mutation = mutEntry.getValue();

                store.mutate(key, mutation.getAdditions(), mutation.getDeletions(), txh);
            }
        }

	}

	private Cluster connect() {
		Cluster.Builder builder = Cluster.builder().addContactPoints(hostnames);
		builder.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnectionsPerHost);

        Cluster cluster = builder.build();
		Metadata metadata = cluster.getMetadata();
		logger.info("Connected to cluster: {}", metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			logger.info("DC: {}; Host: {}; Rack: {}", host.getDatacenter(), host.getAddress().toString(), host.getRack());
		}

        return cluster;
    }

	private void createKeyspace(String name) {
		Session session = cluster.connect();
        try {
            session.execute("CREATE KEYSPACE " + name + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':" + String.valueOf(replicationFactor) + "};");
        } finally {
            session.close();
        }
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

		if (!isTablePresent)
			createTable(name);
	}

	private void createTable(String name) {
		Session session = cluster.connect(keySpaceName);

        StringBuilder ct = new StringBuilder("CREATE TABLE ").append(name)
                                .append(" (key blob, c blob, v blob, PRIMARY KEY(key, c)) ")
                                .append("WITH compression={");


        if (compressionEnabled) {
            ct.append("'sstable_compression': '").append(compressionClass).append("', ")
                  .append("'chunk_length_kb': ").append(Integer.toString(compressionChunkSizeKB));
        }

        ct.append("}");

        try {
            session.execute(ct.toString());
        } finally {
            session.close();
        }
	}

	@Override
	public String getName() {
		return keySpaceName;
	}

}
