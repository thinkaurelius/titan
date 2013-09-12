package com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory backend storage engine.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryStoreManager implements KeyColumnValueStoreManager {

    private final ConcurrentHashMap<String,InMemoryKeyColumnValueStore> stores;

    private final StoreFeatures features;
    private final Map<String,String> storeConfig;

    public InMemoryStoreManager() {
        this(new BaseConfiguration());
    }

    public InMemoryStoreManager(final Configuration configuration) {

        stores = new ConcurrentHashMap<String,InMemoryKeyColumnValueStore>();
        storeConfig = new ConcurrentHashMap<String,String>();

        features = new StoreFeatures();
        features.supportsScan = true;
        features.supportsBatchMutation = false;
        features.supportsTransactions = false;
        features.supportsConsistentKeyOperations = true;
        features.supportsLocking = false;
        features.isDistributed = false;

        features.isKeyOrdered = true;
        features.hasLocalKeyPartition = false;
    }

    // This implementation ignores timestamp (present for interface compatibility)
    @Override
    public StoreTransaction beginTransaction(ConsistencyLevel consistencyLevel, Long timestamp)
            throws StorageException {
        return beginTransaction(consistencyLevel);
    }

    @Override
    public StoreTransaction beginTransaction(ConsistencyLevel consistencyLevel) throws StorageException {
        return new TransactionHandle(consistencyLevel);
    }

    @Override
    public void close() throws StorageException {
        for (InMemoryKeyColumnValueStore store : stores.values()) {
            store.close();
        }
        stores.clear();
    }

    @Override
    public void clearStorage() throws StorageException {
        for (InMemoryKeyColumnValueStore store : stores.values()) {
            store.clear();
        }
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public String getConfigurationProperty(String key) throws StorageException {
        return storeConfig.get(key);
    }

    @Override
    public void setConfigurationProperty(String key, String value) throws StorageException {
        storeConfig.put(key,value);
    }

    @Override
    public KeyColumnValueStore openDatabase(final String name) throws StorageException {
        if (!stores.containsKey(name)) {
            stores.putIfAbsent(name,new InMemoryKeyColumnValueStore(name));
        }
        KeyColumnValueStore store = stores.get(name);
        Preconditions.checkNotNull(store);
        return store;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        for (Map.Entry<String,Map<StaticBuffer, KCVMutation>> storeMut : mutations.entrySet()) {
            KeyColumnValueStore store = stores.get(storeMut.getKey());
            Preconditions.checkNotNull(store);
            for (Map.Entry<StaticBuffer,KCVMutation> keyMut : storeMut.getValue().entrySet()) {
                store.mutate(keyMut.getKey(),keyMut.getValue().getAdditions(),keyMut.getValue().getDeletions(),txh);
            }
        }
    }

    @Override
    public String getName() {
        return toString();
    }

    private class TransactionHandle extends AbstractStoreTransaction {

        public TransactionHandle(ConsistencyLevel level) {
            super(level);
        }
    }
}
