package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.foundationdb.FDBException;
import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.tuple.ByteArrayUtil;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import static com.thinkaurelius.titan.diskstorage.foundationdb.FoundationDBTransaction.wrapException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class FoundationDBKeyValueStore implements OrderedKeyValueStore {
    private static final Logger logger = LoggerFactory.getLogger(FoundationDBKeyValueStore.class);

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
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        try {
            return getTransaction(txh).get(getKeyBytes(key)).get() != null;
        }
        catch (FDBException e) {
            throw wrapException(e);
        }
    }

    class RangeRecordIterator implements RecordIterator<KeyValueEntry> {
        private final Transaction transaction;
        private final byte[] endKey;
        private final KeySelector selector;
        private Iterator<KeyValue> range;
        private int count, limit;
        private byte[] lastKey;
        private KeyValueEntry next = null;

        public RangeRecordIterator(Transaction transaction,
                                   byte[] startKey, byte[] endKey,
                                   int limit, KeySelector selector) {
            this.transaction = transaction;
            this.endKey = endKey;
            this.limit = limit;
            this.selector = selector;
            this.range = transaction.getRange(startKey, endKey, limit).iterator();
        }

        private boolean fillNext() {
            if (next != null) return true;
            if (selector.reachedLimit()) return false;
            while (true) {
                if (range.hasNext()) {
                    KeyValue kv = range.next();
                    count++;
                    lastKey = kv.getKey();
                    StaticBuffer k = getKeyBuffer(kv);
                    if (selector.include(k)) {
                        next = new KeyValueEntry(k, getValueBuffer(kv));
                        return true;
                    }
                }
                else if ((limit == Transaction.ROW_LIMIT_UNLIMITED) ||
                         (count < limit)) {
                    // Exhausted with this limit; would exhaust with a greater one, too.
                    return false;
                }
                else {
                    // Range has exhausted due to requested limit, but selector
                    // hasn't reached that limit. It must be filtering, so need to
                    // get some more, continuing from the last one found.
                    count = 0;
                    limit = Transaction.ROW_LIMIT_UNLIMITED;
                    range = transaction.getRange(com.foundationdb.KeySelector.firstGreaterThan(lastKey),
                                                 com.foundationdb.KeySelector.firstGreaterOrEqual(endKey),
                                                 limit).iterator();
                }
            }
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
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        logger.trace("Slice {} {}: {}", txh, storeName, query);

        return new RangeRecordIterator(getTransaction(txh),
                                       getKeyBytes(query.getStart()),
                                       getKeyBytes(query.getEnd()),
                                       query.hasLimit() ? query.getLimit() : Transaction.ROW_LIMIT_UNLIMITED,
                                       query.getKeySelector());
    }

    @Override
    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        logger.trace("Slices {} {}: {}", txh, storeName, queries);

        Map<KVQuery,RecordIterator<KeyValueEntry>> result = new HashMap<KVQuery,RecordIterator<KeyValueEntry>>(queries.size());
        for (KVQuery query : queries) {
            // NOTE: This succeeds in initiating the queries in parallel because a
            // request is sent right away from getRange() but nothing blocks until
            // hasNext() on an Iterator.
            result.put(query, getSlice(query, txh));
        }
        return result;
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        logger.trace("Get {} {}: {}", txh, storeName, key);
        try {
            byte[] result = getTransaction(txh).get(getKeyBytes(key)).get();
            if (result == null) return null;
            else return getBuffer(result);
        }
        catch (FDBException e) {
            throw wrapException(e);
        }
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws BackendException {
        logger.trace("Insert {} {}: {} = {}", txh, storeName, key, value);
        try {
            getTransaction(txh).set(getKeyBytes(key), getBytes(value));
        }
        catch (FDBException e) {
            throw wrapException(e);
        }
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        logger.trace("Delete {} {}: {}", txh, storeName, key);
        try {
            getTransaction(txh).clear(getKeyBytes(key));
        }
        catch (FDBException e) {
            throw wrapException(e);
        }
    }

    public void mutate(KVMutation mutation, StoreTransaction txh) throws BackendException {
        if (logger.isTraceEnabled()) {
            StringBuilder str = new StringBuilder();
            for (StaticBuffer key : mutation.getDeletions()) {
                str.append("\n Clear ").append(key);
            }
            for (KeyValueEntry kv : mutation.getAdditions()) {
                str.append("\n Set ").append(kv.getKey()).append(" = ").append(kv.getValue());
            }
            logger.trace("Mutate {} {}: {}", txh, storeName, str);
        }
        try {
            for (StaticBuffer key : mutation.getDeletions()) {
                getTransaction(txh).clear(getKeyBytes(key));
            }
            for (KeyValueEntry kv : mutation.getAdditions()) {
                getTransaction(txh).set(getKeyBytes(kv.getKey()), getBytes(kv.getValue()));
            }
        }
        catch (FDBException e) {
            throw wrapException(e);
        }
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        // Not needed.
    }

    @Override
    public String getName() {
        return storeName;
    }

    @Override
    public void close() throws BackendException {
        manager.removeStore(this);
    }

    private static Transaction getTransaction(StoreTransaction txh) {
        return ((FoundationDBTransaction) txh).getTransaction();
    }

}
