package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.diskstorage.indexing.IndexTransaction;
import com.thinkaurelius.titan.diskstorage.indexing.KeyInformation;
import com.thinkaurelius.titan.diskstorage.indexing.RawQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bundles all transaction handles from the various backend systems and provides a proxy for some of their
 * methods for convenience.
 * Also increases robustness of read call by attempting read calls multiple times on failure.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class BackendTransaction implements TransactionHandle {

    private static final Logger log =
            LoggerFactory.getLogger(BackendTransaction.class);

    public static final int MIN_TASKS_TO_PARALLELIZE = 2;

    //Assumes 64 bit key length as specified in IDManager
    public static final StaticBuffer EDGESTORE_MIN_KEY = ByteBufferUtil.zeroBuffer(8);
    public static final StaticBuffer EDGESTORE_MAX_KEY = ByteBufferUtil.oneBuffer(8);

    private final StoreTransaction storeTx;
    private final StoreFeatures storeFeatures;

    private final KeyColumnValueStore edgeStore;
    private final KeyColumnValueStore vertexIndexStore;
    private final KeyColumnValueStore edgeIndexStore;

    private final int maxReadRetryAttempts;
    private final int retryStorageWaitTime;

    private final Executor threadPool;

    private final Map<String, IndexTransaction> indexTx;

    public BackendTransaction(StoreTransaction storeTx, StoreFeatures features,
                              KeyColumnValueStore edgeStore,
                              KeyColumnValueStore vertexIndexStore, KeyColumnValueStore edgeIndexStore,
                              int maxReadRetryAttempts, int retryStorageWaitTime,
                              Map<String, IndexTransaction> indexTx, Executor threadPool) {
        this.storeTx = storeTx;
        this.storeFeatures = features;
        this.edgeStore = edgeStore;
        this.vertexIndexStore = vertexIndexStore;
        this.edgeIndexStore = edgeIndexStore;
        this.maxReadRetryAttempts = maxReadRetryAttempts;
        this.retryStorageWaitTime = retryStorageWaitTime;
        this.indexTx = indexTx;
        this.threadPool = threadPool;
    }

    public StoreTransaction getStoreTransactionHandle() {
        return storeTx;
    }

    public IndexTransaction getIndexTransactionHandle(String index) {
        Preconditions.checkArgument(StringUtils.isNotBlank(index));
        IndexTransaction itx = indexTx.get(index);
        Preconditions.checkNotNull(itx, "Unknown index: " + index);
        return itx;
    }

    @Override
    public void commit() throws StorageException {
        storeTx.commit();
        for (IndexTransaction itx : indexTx.values()) itx.commit();
    }

    @Override
    public void rollback() throws StorageException {
        storeTx.rollback();
        for (IndexTransaction itx : indexTx.values()) itx.rollback();
    }

    @Override
    public void flush() throws StorageException {
        storeTx.flush();
        for (IndexTransaction itx : indexTx.values()) itx.flush();
    }

    /* ###################################################
            Convenience Write Methods
     */

    /**
     * Applies the specified insertion and deletion mutations on the edge store to the provided key.
     * Both, the list of additions or deletions, may be empty or NULL if there is nothing to be added and/or deleted.
     *
     * @param key       Key
     * @param additions List of entries (column + value) to be added
     * @param deletions List of columns to be removed
     */
    public void mutateEdges(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions) throws StorageException {
        edgeStore.mutate(key, additions, deletions, storeTx);
    }

    /**
     * Applies the specified insertion and deletion mutations on the property index to the provided key.
     * Both, the list of additions or deletions, may be empty or NULL if there is nothing to be added and/or deleted.
     *
     * @param key       Key
     * @param additions List of entries (column + value) to be added
     * @param deletions List of columns to be removed
     */
    public void mutateVertexIndex(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions) throws StorageException {
        vertexIndexStore.mutate(key, additions, deletions, storeTx);
    }

    public void mutateEdgeIndex(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions) throws StorageException {
        edgeIndexStore.mutate(key, additions, deletions, storeTx);
    }

    /**
     * Acquires a lock for the key-column pair on the edge store which ensures that nobody else can take a lock on that
     * respective entry for the duration of this lock (but somebody could potentially still overwrite
     * the key-value entry without taking a lock).
     * The expectedValue defines the value expected to match the value at the time the lock is acquired (or null if it is expected
     * that the key-column pair does not exist).
     * <p/>
     * If this method is called multiple times with the same key-column pair in the same transaction, all but the first invocation are ignored.
     * <p/>
     * The lock has to be released when the transaction closes (commits or aborts).
     *
     * @param key           Key on which to lock
     * @param column        Column the column on which to lock
     * @param expectedValue The expected value for the specified key-column pair on which to lock. Null if it is expected that the pair does not exist
     */
    public void acquireEdgeLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue) throws StorageException {
        edgeStore.acquireLock(key, column, expectedValue, storeTx);
    }

    /**
     * Acquires a lock for the key-column pair on the property index which ensures that nobody else can take a lock on that
     * respective entry for the duration of this lock (but somebody could potentially still overwrite
     * the key-value entry without taking a lock).
     * The expectedValue defines the value expected to match the value at the time the lock is acquired (or null if it is expected
     * that the key-column pair does not exist).
     * <p/>
     * If this method is called multiple times with the same key-column pair in the same transaction, all but the first invocation are ignored.
     * <p/>
     * The lock has to be released when the transaction closes (commits or aborts).
     *
     * @param key           Key on which to lock
     * @param column        Column the column on which to lock
     * @param expectedValue The expected value for the specified key-column pair on which to lock. Null if it is expected that the pair does not exist
     */
    public void acquireVertexIndexLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue) throws StorageException {
        vertexIndexStore.acquireLock(key, column, expectedValue, storeTx);
    }

    /* ###################################################
            Convenience Read Methods
     */

    public List<Entry> edgeStoreQuery(final KeySliceQuery query) {
        return executeRead(new Callable<List<Entry>>() {
            @Override
            public List<Entry> call() throws Exception {
                return edgeStore.getSlice(query, storeTx);
            }

            @Override
            public String toString() {
                return "EdgeStoreQuery";
            }
        });
    }

    public List<List<Entry>> edgeStoreMultiQuery(final List<StaticBuffer> keys, final SliceQuery query) {
        if (storeFeatures.supportsMultiQuery()) {
            return executeRead(new Callable<List<List<Entry>>>() {
                @Override
                public List<List<Entry>> call() throws Exception {
                    return edgeStore.getSlice(keys, query, storeTx);
                }

                @Override
                public String toString() {
                    return "MultiEdgeStoreQuery";
                }
            });
        } else {
            final List<List<Entry>> results;
            if (threadPool == null || keys.size() < MIN_TASKS_TO_PARALLELIZE) {
                results = new ArrayList<List<Entry>>(keys.size());
                for (StaticBuffer key : keys) {
                    results.add(edgeStoreQuery(new KeySliceQuery(key, query)));
                }
            } else {
                final CountDownLatch doneSignal = new CountDownLatch(keys.size());
                final AtomicInteger failureCount = new AtomicInteger(0);
                List<Entry>[] resultArray = new List[keys.size()];
                for (int i = 0; i < keys.size(); i++) {
                    threadPool.execute(new SliceQueryRunner(new KeySliceQuery(keys.get(i), query),
                            doneSignal, failureCount, resultArray, i));
                }
                try {
                    doneSignal.await();
                } catch (InterruptedException e) {
                    throw new TitanException("Interrupted while waiting for multi-query to complete", e);
                }
                if (failureCount.get() > 0) {
                    throw new TitanException("Could not successfully complete multi-query. " + failureCount.get() + " individual queries failed.");
                }
                results = Arrays.asList(resultArray);
                assert keys.size() == results.size();
                for (List l : results) assert l != null;
            }
            return results;
        }
    }

    private class SliceQueryRunner implements Runnable {

        final KeySliceQuery kq;
        final CountDownLatch doneSignal;
        final AtomicInteger failureCount;
        final Object[] resultArray;
        final int resultPosition;

        private SliceQueryRunner(KeySliceQuery kq, CountDownLatch doneSignal, AtomicInteger failureCount,
                                 Object[] resultArray, int resultPosition) {
            this.kq = kq;
            this.doneSignal = doneSignal;
            this.failureCount = failureCount;
            this.resultArray = resultArray;
            this.resultPosition = resultPosition;
        }

        @Override
        public void run() {
            try {
                List<Entry> result;
                if (maxReadRetryAttempts > 1)
                    result = edgeStoreQuery(kq);
                else //Premature optimization
                    result = edgeStore.getSlice(kq, storeTx);
                resultArray[resultPosition] = result;
            } catch (Exception e) {
                failureCount.incrementAndGet();
                log.warn("Individual query in multi-transaction failed: ", e);
            } finally {
                doneSignal.countDown();
            }
        }
    }

    public boolean edgeStoreContainsKey(final StaticBuffer key) {
        return executeRead(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return edgeStore.containsKey(key, storeTx);
            }

            @Override
            public String toString() {
                return "EdgeStoreContainsKey";
            }
        });
    }

    public KeyIterator edgeStoreKeys(final SliceQuery sliceQuery) {
        if (!storeFeatures.supportsScan())
            throw new UnsupportedOperationException("The configured storage backend does not support global graph operations - use Faunus instead");

        return executeRead(new Callable<KeyIterator>() {
            @Override
            public KeyIterator call() throws Exception {
                return (storeFeatures.isKeyOrdered())
                        ? edgeStore.getKeys(new KeyRangeQuery(EDGESTORE_MIN_KEY, EDGESTORE_MAX_KEY, sliceQuery), storeTx)
                        : edgeStore.getKeys(sliceQuery, storeTx);
            }

            @Override
            public String toString() {
                return "EdgeStoreKeys";
            }
        });
    }

    public KeyIterator edgeStoreKeys(final KeyRangeQuery range) {
        Preconditions.checkArgument(storeFeatures.supportsOrderedScan(), "The configured storage backend does not support ordered scans");

        return executeRead(new Callable<KeyIterator>() {
            @Override
            public KeyIterator call() throws Exception {
                return edgeStore.getKeys(range, storeTx);
            }

            @Override
            public String toString() {
                return "EdgeStoreKeys";
            }
        });
    }

    public List<Entry> vertexIndexQuery(final KeySliceQuery query) {
        return executeRead(new Callable<List<Entry>>() {
            @Override
            public List<Entry> call() throws Exception {
                return vertexIndexStore.getSlice(query, storeTx);
            }

            @Override
            public String toString() {
                return "VertexIndexQuery";
            }
        });

    }

    public List<Entry> edgeIndexQuery(final KeySliceQuery query) {
        return executeRead(new Callable<List<Entry>>() {
            @Override
            public List<Entry> call() throws Exception {
                return edgeIndexStore.getSlice(query, storeTx);
            }

            @Override
            public String toString() {
                return "EdgeIndexQuery";
            }
        });
    }

    public List<String> indexQuery(final String index, final IndexQuery query) {
        final IndexTransaction indexTx = getIndexTransactionHandle(index);
        return executeRead(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                return indexTx.query(query);
            }

            @Override
            public String toString() {
                return "IndexQuery";
            }
        });
    }

    public Iterable<RawQuery.Result<String>> rawQuery(final String index, final RawQuery query) {
        final IndexTransaction indexTx = getIndexTransactionHandle(index);
        return executeRead(new Callable<Iterable<RawQuery.Result<String>>>() {
            @Override
            public Iterable<RawQuery.Result<String>> call() throws Exception {
                return indexTx.query(query);
            }

            @Override
            public String toString() {
                return "RawQuery";
            }
        });
    }


    private final <V> V executeRead(Callable<V> exe) throws TitanException {
        return BackendOperation.execute(exe, maxReadRetryAttempts, retryStorageWaitTime);
    }


}
