package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Experimental Accumulo store.
 * <p/>
 * This is not ready for production.  It's pretty slow.
 * <p/>
 * Here are some areas that might need work:
 * <p/>
 * - batching? (consider HTable#batch, HTable#setAutoFlush(false)
 * - tuning HTable#setWriteBufferSize (?)
 * - writing a server-side filter to replace ColumnCountGetFilter, which drops
 * all columns on the row where it reaches its limit.  This requires getSlice,
 * currently, to impose its limit on the client side.  That obviously won't
 * scale.
 * - RowMutations for combining Puts+Deletes (need a newer HBase than 0.92 for this)
 * - (maybe) fiddle with HTable#setRegionCachePrefetch and/or #prewarmRegionCache
 * <p/>
 * There may be other problem areas.  These are just the ones of which I'm aware.
 */
public class AccumuloKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloKeyColumnValueStore.class);

    private final String tableName;

    private final String columnFamily;
    // This is columnFamily.getBytes()
    private final byte[] columnFamilyBytes;

    AccumuloKeyColumnValueStore(String tableName, String columnFamily) {
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.columnFamilyBytes = columnFamily.getBytes();
    }

    @Override
    public void close() throws StorageException {
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        return false;
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        return Collections.emptyList();
    }

    @Override
    public void mutate(StaticBuffer key,
                       List<Entry> additions,
                       List<StaticBuffer> deletions,
                       StoreTransaction txh) throws StorageException {
    }

    @Override
    public void acquireLock(StaticBuffer key,
                            StaticBuffer column,
                            StaticBuffer expectedValue,
                            StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    /**
     *
     * IMPORTANT: Makes the assumption that all keys are 8 byte longs
     *
     * @param txh
     * @return
     * @throws StorageException
     */
    @Override
    public RecordIterator<StaticBuffer> getKeys(StoreTransaction txh) throws StorageException {
        return null;
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return columnFamily;
    }
}
