package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Wraps a {@link OrderedKeyValueStoreManager} and exposes it as a {@link KeyColumnValueStoreManager}.
 *
 * An optional mapping of key-length can be defined if it is known that the {@link KeyColumnValueStore} of a given
 * name has a static key length. See {@link OrderedKeyValueStoreAdapter} for more information.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public class OrderedKeyValueStoreManagerAdapter implements KeyColumnValueStoreManager {


    private final OrderedKeyValueStoreManager manager;

    private final ImmutableMap<String, Integer> keyLengths;

    private final Map<String,OrderedKeyValueStoreAdapter> stores;

    public OrderedKeyValueStoreManagerAdapter(OrderedKeyValueStoreManager manager) {
        this(manager, new HashMap<String,Integer>());
    }

    public OrderedKeyValueStoreManagerAdapter(OrderedKeyValueStoreManager manager, Map<String, Integer> keyLengths) {
        this.manager = manager;
        ImmutableMap.Builder<String, Integer> mb = ImmutableMap.builder();
        if (keyLengths != null && !keyLengths.isEmpty()) mb.putAll(keyLengths);
        this.keyLengths = mb.build();
        this.stores = new HashMap<String,OrderedKeyValueStoreAdapter>();
    }

    public StoreFeatures getFeatures() {
        return manager.getFeatures();
    }

    // This implementation ignores timestamp (present for interface compatibility)
    @Override
    public StoreTransaction beginTransaction(ConsistencyLevel level, Long timestamp)
            throws StorageException {
        return beginTransaction(level);
    }

    @Override
    public StoreTransaction beginTransaction(ConsistencyLevel level) throws StorageException {
        return manager.beginTransaction(level);
    }

    @Override
    public void close() throws StorageException {
        manager.close();
    }

    @Override
    public void clearStorage() throws StorageException {
        manager.clearStorage();
    }

    @Override
    public synchronized OrderedKeyValueStoreAdapter openDatabase(String name)
            throws StorageException {
        if (!stores.containsKey(name)) {
            OrderedKeyValueStoreAdapter store = wrapKeyValueStore(manager.openDatabase(name), keyLengths);
            stores.put(name,store);
        }
        return stores.get(name);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        Map<String,KVMutation> converted = new HashMap<String,KVMutation>(mutations.size());
        for (Map.Entry<String,Map<StaticBuffer, KCVMutation>> storeEntry : mutations.entrySet()) {
            OrderedKeyValueStoreAdapter store = openDatabase(storeEntry.getKey());
            Preconditions.checkNotNull(store);

            KVMutation mut = new KVMutation();
            for (Map.Entry<StaticBuffer,KCVMutation> entry : storeEntry.getValue().entrySet()) {
                StaticBuffer key = entry.getKey();
                KCVMutation mutation = entry.getValue();
                if (mutation.hasAdditions()) {
                    for (Entry addition : mutation.getAdditions()) {
                        mut.addition(new KeyValueEntry(store.concatenate(key,addition.getColumn()),addition.getValue()));
                    }
                }

                if (mutation.hasDeletions()) {
                    for (StaticBuffer column : mutation.getDeletions()) {
                        mut.deletion(store.concatenate(key,column));
                    }
                }
            }
            converted.put(storeEntry.getKey(), mut);
        }
        manager.mutateMany(converted,txh);
    }

    private static final OrderedKeyValueStoreAdapter wrapKeyValueStore(OrderedKeyValueStore store, Map<String, Integer> keyLengths) {
        String name = store.getName();
        if (keyLengths.containsKey(name)) {
            int keyLength = keyLengths.get(name);
            Preconditions.checkArgument(keyLength > 0);
            return new OrderedKeyValueStoreAdapter(store, keyLength);
        } else {
            return new OrderedKeyValueStoreAdapter(store);
        }
    }

    @Override
    public String getConfigurationProperty(final String key) throws StorageException {
        return manager.getConfigurationProperty(key);
    }

    @Override
    public void setConfigurationProperty(final String key, final String value) throws StorageException {
        manager.setConfigurationProperty(key, value);
    }
    
    @Override
    public String getName() {
        return manager.getName();
    }
}
