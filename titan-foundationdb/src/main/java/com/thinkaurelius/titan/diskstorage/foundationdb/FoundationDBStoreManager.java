package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.FDBException;
import com.foundationdb.Transaction;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FoundationDBStoreManager extends AbstractStoreManager implements OrderedKeyValueStoreManager {
    private static final Logger logger = LoggerFactory.getLogger(FoundationDBStoreManager.class);

    public static final ConfigOption<String> DIRECTORY_NAME = new ConfigOption<String>(STORAGE_NS,"fdb-directory",
            "Name of directory subspace",
            ConfigOption.Type.LOCAL, "titan");
    public static final ConfigOption<String> CLUSTER_FILE = new ConfigOption<String>(STORAGE_NS,"clusterfile",
            "FDB cluster file override",
            ConfigOption.Type.LOCAL, "NONE");
    public static final ConfigOption<String> TRACE_DIRECTORY = new ConfigOption<String>(STORAGE_NS,"trace-directory",
            "Location to write trace files",
            ConfigOption.Type.LOCAL, "NONE");

    private final Database db;
    private final ConcurrentHashMap<String, FoundationDBKeyValueStore> openStores;
    private final StoreFeatures features;

    public final String dirname;

    private final DirectorySubspace directory;

    public FoundationDBStoreManager(Configuration config) {
        super(config);

        dirname = config.get(DIRECTORY_NAME);
        String clusterFile = config.get(CLUSTER_FILE);
        String traceDirectory = config.get(TRACE_DIRECTORY);

        FDB fdb = FDB.selectAPIVersion(200);
        if (!traceDirectory.equals("NONE")) {
            fdb.options().setTraceEnable(traceDirectory);
        }
        if (clusterFile.equals("NONE")) {
            db = fdb.open();
        }
        else {
            db = fdb.open(clusterFile);
        }

        directory = new DirectoryLayer().createOrOpen(db, Arrays.asList(dirname)).get();

        openStores = new ConcurrentHashMap<String, FoundationDBKeyValueStore>();

        features = new StandardStoreFeatures.Builder()
            .unorderedScan(true)
            .orderedScan(true)
            .batchMutation(true)
            .multiQuery(true)
            .transactional(true)
            .keyConsistent(GraphDatabaseConfiguration.buildConfiguration())
            .locking(true)
            .keyOrdered(true)
            .distributed(true)
            .build();

        logger.trace("Create {}", dirname);
    }

    @Override
    public FoundationDBKeyValueStore openDatabase(String name) throws BackendException {
        FoundationDBKeyValueStore kv = openStores.get(name);

        if (kv == null) {
            DirectorySubspace subspace = directory.createOrOpen(db, Arrays.asList(name)).get();
            FoundationDBKeyValueStore newkv = new FoundationDBKeyValueStore(name, subspace, this);
            kv = openStores.putIfAbsent(name, newkv);

            if (kv == null) kv = newkv;

            logger.trace("Open {}: {}", dirname, name);
        }

        return kv;
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        for (Map.Entry<String, KVMutation> entry: mutations.entrySet()) {
            FoundationDBKeyValueStore store = openDatabase(entry.getKey());
            store.mutate(entry.getValue(), txh);
        }
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        Transaction tr = db.createTransaction();
        return new FoundationDBTransaction(tr, config);
    }

    public void removeStore(FoundationDBKeyValueStore kv) {
        openStores.remove(kv.getName());
    }

    @Override
    public void close() throws BackendException {
        logger.trace("Close {}", dirname);
        openStores.clear();
        db.dispose();
    }

    @Override
    public void clearStorage() throws BackendException {
        logger.trace("Clear {}", dirname);
        try {
            directory.remove(db).get();
            close();
        }
        catch (FDBException e) {
            throw FoundationDBTransaction.wrapException(e);
        }
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + ":" + dirname;
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

}
