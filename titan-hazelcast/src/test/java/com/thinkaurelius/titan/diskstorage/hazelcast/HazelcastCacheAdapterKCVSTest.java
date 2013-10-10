package com.thinkaurelius.titan.diskstorage.hazelcast;

import com.thinkaurelius.titan.HazelcastStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.CacheStoreManagerAdapter;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NAMESPACE;

public class HazelcastCacheAdapterKCVSTest extends KeyColumnValueStoreTest {

    public HazelcastCacheAdapterKCVSTest() throws StorageException {
        manager = openStorageManager();
        store = manager.openDatabase(storeName);
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new CacheStoreManagerAdapter(
                new HazelcastCacheStoreManager(
                        HazelcastStorageSetup.getHazelcastBaseConfig()));
    }

    @Override
    public void setUp() throws StorageException {
        tx = manager.beginTransaction(new StoreTxConfig());
    }

    @Override
    public void tearDown() throws Exception {
        if (tx != null)
            tx.commit();
        manager.clearStorage();
    }

    @Override
    public void clopen() {
    }

    @Override
    public void close() {
    }
}
