package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.tuple.ByteArrayUtil;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class FoundationDBKeyValueStore implements OrderedKeyValueStore {

    private final String storeName;
    private final byte[] prefix;
    private final FoundationDBStoreManager manager;

    public FoundationDBKeyValueStore(String name, DirectorySubspace subspace, FoundationDBStoreManager m) {
        storeName = name;
        prefix = subspace.getKey();
        manager = m;
    }

    private byte[] getBytes(StaticBuffer buffer) {
        ByteBuffer byteBuffer = buffer.asByteBuffer();
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        byteBuffer.rewind();
        return bytes;
    }

    private byte[] getKeyBytes(StaticBuffer buffer) {
        return ByteArrayUtil.join(prefix, getBytes(buffer));
    }

    private StaticBuffer getBuffer(byte[] bytes) {
        return new StaticArrayBuffer(bytes, 0, bytes.length);
    }

    private StaticBuffer getKeyBuffer(KeyValue kv) {
        byte[] fullKey = kv.getKey();
        return new StaticArrayBuffer(fullKey, prefix.length, fullKey.length);
    }

    private StaticBuffer getValueBuffer(KeyValue kv) {
        byte[] value = kv.getValue();
        return new StaticArrayBuffer(value, 0, value.length);
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return getTransaction(txh).get(getKeyBytes(key)).get() != null;
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(StaticBuffer keyStart, StaticBuffer keyEnd, final KeySelector selector, StoreTransaction txh) throws StorageException {
        // TODO: I don't think it's possible to apply a numeric limit from the
        // selector to the range, since it after any filtering.

        final Iterator<KeyValue> range = getTransaction(txh)
            .getRange(getKeyBytes(keyStart), getKeyBytes(keyEnd))
            .iterator();

        if (selector == null) {
            return new RecordIterator<KeyValueEntry>() {
                @Override
                public boolean hasNext() {
                    return range.hasNext();
                }

                @Override
                public KeyValueEntry next() {
                    KeyValue kv = range.next();
                    return new KeyValueEntry(getKeyBuffer(kv), getValueBuffer(kv));
                }

                @Override
                public void close() {
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
        else {
            return new RecordIterator<KeyValueEntry>() {
                private KeyValueEntry next = null;
                private boolean reachedLimit = false;

                private boolean fillNext() {
                    if (next != null) return true;
                    if (reachedLimit) return false;
                    while (range.hasNext()) {
                        KeyValue kv = range.next();
                        StaticBuffer k = getKeyBuffer(kv);
                        if (selector.include(k)) {
                            next = new KeyValueEntry(k, getValueBuffer(kv));
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public boolean hasNext() {
                    return fillNext();
                }

                @Override
                public KeyValueEntry next() {
                    if (fillNext()) {
                        KeyValueEntry result = next;
                        next = null;
                        reachedLimit = selector.reachedLimit();
                        return result;
                    }
                    throw new NoSuchElementException();
                }

                @Override
                public void close() {
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws StorageException {
        byte[] result = getTransaction(txh).get(getKeyBytes(key)).get();
        if (result == null) return null;
        else return getBuffer(result);
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws StorageException {
        getTransaction(txh).set(getKeyBytes(key), getBytes(value));
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws StorageException {
        getTransaction(txh).clear(getKeyBytes(key));
    }

    public void mutate(KVMutation mutation, StoreTransaction txh) throws StorageException {
        for (StaticBuffer key : mutation.getDeletions()) {
            getTransaction(txh).clear(getKeyBytes(key));
        }
        for (KeyValueEntry kv : mutation.getAdditions()) {
            getTransaction(txh).set(getKeyBytes(kv.getKey()), getBytes(kv.getValue()));
        }
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return storeName;
    }

    @Override
    public void close() throws StorageException {
        manager.removeStore(this);
    }

    private static Transaction getTransaction(StoreTransaction txh) {
        return ((FoundationDBTransaction) txh).getTransaction();
    }

}
