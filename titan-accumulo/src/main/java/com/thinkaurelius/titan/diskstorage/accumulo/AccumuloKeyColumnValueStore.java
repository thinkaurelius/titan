package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRangeQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import javax.annotation.Nullable;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.accumulo.core.iterators.user.ColumnRangeFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

/**
 * Experimental Accumulo store.
 * <p/>
 * This is not ready for production. It's pretty slow.
 * <p/>
 * Here are some areas that might need work:
 * <p/>
 * - batching? (consider HTable#batch, HTable#setAutoFlush(false) - tuning
 * HTable#setWriteBufferSize (?) - writing a server-side filter to replace
 * ColumnCountGetFilter, which drops all columns on the row where it reaches its
 * limit. This requires getSlice, currently, to impose its limit on the client
 * side. That obviously won't scale. - RowMutations for combining Puts+Deletes
 * (need a newer HBase than 0.92 for this) - (maybe) fiddle with
 * HTable#setRegionCachePrefetch and/or #prewarmRegionCache
 * <p/>
 * There may be other problem areas. These are just the ones of which I'm aware.
 */
public class AccumuloKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloKeyColumnValueStore.class);
    // Default deleter, scanner, writer parameters 
    private static final Authorizations DEFAULT_AUTHORIZATIONS = new Authorizations();
    private static final Long DEFAULT_MAX_MEMORY = 50 * 1024 * 1024l;
    private static final Long DEFAULT_MAX_LATENCY = 100l;
    private static final Integer DEFAULT_MAX_QUERY_THREADS = 10;
    private static final Integer DEFAULT_MAX_WRITE_THREADS = 10;
    // Instance variables
    private final Connector connector;  // thread-safe
    private final String tableName;
    private final String columnFamily;
    // This is columnFamily.getBytes()
    private final byte[] columnFamilyBytes;
    private final Text columnFamilyText;
    private final boolean clientSideIterators;

    AccumuloKeyColumnValueStore(Connector connector, String tableName, String columnFamily, boolean clientSideIterators) {
        this.connector = connector;
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.columnFamilyBytes = columnFamily.getBytes();
        this.columnFamilyText = new Text(columnFamily);
        this.clientSideIterators = clientSideIterators;
    }

    @Override
    public String getName() {
        return columnFamily;
    }

    @Override
    public void close() throws StorageException {
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        Scanner scanner;
        try {
            scanner = connector.createScanner(tableName, DEFAULT_AUTHORIZATIONS);
        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }

        byte[] keyBytes = key.as(StaticBuffer.ARRAY_FACTORY);
        scanner.setRange(new Range(new Text(keyBytes)));
        scanner.fetchColumnFamily(columnFamilyText);

        return scanner.iterator().hasNext();
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        Scanner scanner;
        try {
            scanner = connector.createScanner(tableName, DEFAULT_AUTHORIZATIONS);
        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }

        Text keyText = new Text(query.getKey().as(StaticBuffer.ARRAY_FACTORY));

        Key startKey;
        Key endKey;

        if (query.getSliceStart().length() > 0) {
            startKey = new Key(keyText, columnFamilyText,
                    new Text(query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY)));
        } else {
            startKey = new Key(keyText, columnFamilyText);
        }

        if (query.getSliceEnd().length() > 0) {
            endKey = new Key(keyText, columnFamilyText,
                    new Text(query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY)));
        } else {
            endKey = new Key(keyText, columnFamilyText);
        }

        scanner.setRange(new Range(startKey, true, endKey, false));
        if (query.getLimit() < scanner.getBatchSize()) {
            scanner.setBatchSize(query.getLimit());
        }

        int count = 1;
        List<Entry> entries = new ArrayList<Entry>();
        for (Map.Entry<Key, Value> entry : scanner) {
            if (count > query.getLimit()) {
                break;
            }

            byte[] colQual = entry.getKey().getColumnQualifier().getBytes();
            byte[] value = entry.getValue().get();
            entries.add(StaticBufferEntry.of(new StaticArrayBuffer(colQual), new StaticArrayBuffer(value)));
            count++;
        }

        return entries;
    }

    @Override
    public List<List<Entry>> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        BatchScanner scanner;
        try {
            scanner = connector.createBatchScanner(tableName, DEFAULT_AUTHORIZATIONS, DEFAULT_MAX_QUERY_THREADS);
        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }

        List<Text> keysText = Lists.newArrayList();
        for (StaticBuffer key : keys) {
            keysText.add(new Text(key.as(StaticBuffer.ARRAY_FACTORY)));
        }

        Collection<Range> ranges = Lists.newArrayList();
        for (Text key : keysText) {
            Key startKey;
            Key endKey;

            if (query.getSliceStart().length() > 0) {
                startKey = new Key(key, columnFamilyText,
                        new Text(query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY)));
            } else {
                startKey = new Key(key, columnFamilyText);
            }

            if (query.getSliceEnd().length() > 0) {
                endKey = new Key(key, columnFamilyText,
                        new Text(query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY)));
            } else {
                endKey = new Key(key, columnFamilyText);
            }

            ranges.add(new Range(startKey, true, endKey, false));
        }

        scanner.setRanges(ranges);

        Map<Text, List<Entry>> entries = Maps.newHashMap();
        for (Text key : keysText) {
            entries.put(key, Lists.<Entry>newArrayList());
        }

        int count = 1;
        for (Map.Entry<Key, Value> kv : scanner) {
            if (count > query.getLimit()) {
                break;
            }

            Text key = kv.getKey().getRow();

            byte[] colQual = kv.getKey().getColumnQualifier().getBytes();
            byte[] value = kv.getValue().get();
            Entry entry = StaticBufferEntry.of(new StaticArrayBuffer(colQual), new StaticArrayBuffer(value));

            entries.get(key).add(entry);
            count++;
        }

        List<List<Entry>> results = Lists.newArrayList();
        for (Text key : keysText) {
            results.add(entries.get(key));
        }

        return results;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions,
            StoreTransaction txh) throws StorageException {

        Text keyText = new Text(key.as(StaticBuffer.ARRAY_FACTORY));
        List<Mutation> batch = makeBatch(columnFamilyText, keyText, additions, deletions);

        if (batch.isEmpty()) {
            return; // nothing to apply
        }

        try {
            BatchWriter writer = connector.createBatchWriter(tableName,
                    DEFAULT_MAX_MEMORY, DEFAULT_MAX_LATENCY, DEFAULT_MAX_WRITE_THREADS);
            try {
                writer.addMutations(batch);
                writer.flush();
            } catch (MutationsRejectedException ex) {
                logger.error("Can't write mutations to Titan store " + tableName, ex);
                throw new PermanentStorageException(ex);
            } finally {
                try {
                    writer.close();
                } catch (MutationsRejectedException ex) {
                    logger.error("Can't write mutations to Titan store " + tableName, ex);
                }
            }
        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }
    }

    private static List<Mutation> makeBatch(Text colFamily, Text key, List<Entry> additions, List<StaticBuffer> deletions) {
        if (additions.isEmpty() && deletions.isEmpty()) {
            return Collections.emptyList();
        }

        List<Mutation> batch = new ArrayList<Mutation>(2);

        if (!additions.isEmpty()) {
            Mutation put = makePutMutation(colFamily, key, additions);
            batch.add(put);
        }

        if (!deletions.isEmpty()) {
            Mutation delete = makeDeleteMutation(colFamily, key, deletions);
            batch.add(delete);
        }

        return batch;
    }

    /**
     * Convert deletions to a Delete mutation.
     *
     * @param colFamily The name of the ColumnFamily deletions belong to
     * @param key The row key
     * @param deletions The name of the columns to delete (a.k.a deletions)
     *
     * @return Delete command or null if deletions were null or empty.
     */
    private static Mutation makeDeleteMutation(Text colFamily, Text key, List<StaticBuffer> deletions) {
        Preconditions.checkArgument(!deletions.isEmpty());

        Mutation mutation = new Mutation(new Text(key));
        for (StaticBuffer del : deletions) {
            Text colQual = new Text(del.as(StaticBuffer.ARRAY_FACTORY));
            mutation.putDelete(new Text(colFamily), colQual);
        }
        return mutation;
    }

    /**
     * Convert modification entries into Put command.
     *
     * @param colFamily The name of the ColumnFamily modifications belong to
     * @param key The row key
     * @param modifications The entries to insert/update.
     *
     * @return Put command or null if additions were null or empty.
     */
    private static Mutation makePutMutation(Text colFamily, Text key, List<Entry> modifications) {
        Preconditions.checkArgument(!modifications.isEmpty());

        Mutation mutation = new Mutation(new Text(key));
        for (Entry entry : modifications) {
            Text colQual = new Text(entry.getArrayColumn());
            byte[] value = entry.getArrayValue();
            mutation.put(colFamily, colQual, new Value(value));
        }
        return mutation;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue,
            StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();  // Accumulo stores do not support local key partitions.
    }

    @Override
    public RecordIterator<StaticBuffer> getKeys(StoreTransaction txh) throws StorageException {
        final BatchScanner scanner;

        try {
            scanner = connector.createBatchScanner(tableName,
                    DEFAULT_AUTHORIZATIONS, DEFAULT_MAX_QUERY_THREADS);
        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }

        scanner.setRanges(Collections.singletonList(new Range()));

        scanner.fetchColumnFamily(new Text(columnFamily));

        IteratorSetting firstRowKeyIterator = new IteratorSetting(10, "firstRowKeyIter", FirstEntryInRowIterator.class);
        scanner.addScanIterator(firstRowKeyIterator);

        return new RecordIterator<StaticBuffer>() {
            private final Iterator<Map.Entry<Key, Value>> results = scanner.iterator();

            @Override
            public boolean hasNext() throws StorageException {
                return results.hasNext();
            }

            @Override
            public StaticBuffer next() throws StorageException {
                return new StaticArrayBuffer(results.next().getKey().getRow().getBytes());
            }

            @Override
            public void close() throws StorageException {
                scanner.close();
            }
        };
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws StorageException {
        return executeKeySliceQuery(query.getKeyStart(), query.getKeyEnd(), query);
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException {
        return executeKeySliceQuery(null, null, query);
    }

    private KeyIterator executeKeySliceQuery(StaticBuffer startKey, StaticBuffer endKey,
            SliceQuery columnSlice) throws StorageException {

        Scanner scanner = getKeySliceScanner(startKey, endKey, columnSlice);

        return new RowKeyIterator(scanner);
    }

    private Scanner getKeySliceScanner(StaticBuffer startKey, StaticBuffer endKey, SliceQuery columnSlice) throws StorageException {
        Range range = getRange(startKey, endKey);

        Scanner scanner;
        try {
            scanner = connector.createScanner(tableName, DEFAULT_AUTHORIZATIONS);

        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }

        scanner.setRange(range);
        scanner.fetchColumnFamily(columnFamilyText);

        IteratorSetting columnSliceIterator = null;
        if (columnSlice != null) {
            columnSliceIterator = getColumnSliceIterator(columnSlice);
        }

        if (columnSliceIterator != null) {
            if (clientSideIterators) {
                scanner = new ClientSideIteratorScanner(scanner);
            }
            scanner.addScanIterator(columnSliceIterator);
        }

        return scanner;
    }

    private Range getRange(StaticBuffer startKey, StaticBuffer endKey) {
        Text startRow = null;
        Text endRow = null;

        if (startKey != null && startKey.length() > 0) {
            startRow = new Text(startKey.as(StaticBuffer.ARRAY_FACTORY));
        }

        if (endKey != null && endKey.length() > 0) {
            endRow = new Text(endKey.as(StaticBuffer.ARRAY_FACTORY));
        }

        return new Range(startRow, true, endRow, false);
    }

    private IteratorSetting getColumnSliceIterator(SliceQuery sliceQuery) {
        IteratorSetting is = null;

        byte[] minColumn = sliceQuery.getSliceStart().as(StaticBuffer.ARRAY_FACTORY);
        byte[] maxColumn = sliceQuery.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY);

        if (minColumn.length > 0 || maxColumn.length > 0) {
            is = new IteratorSetting(5, "columnRangeIter", ColumnRangeFilter.class);
            ColumnRangeFilter.setRange(is, minColumn, true, maxColumn, false);
        }

        return is;
    }

    private class RowKeyIterator implements KeyIterator {

        RowIterator rows;
        PeekingIterator<Map.Entry<Key, Value>> currentRow;
        boolean isClosed;

        RowKeyIterator(Scanner scanner) {
            rows = new RowIterator(scanner);
            isClosed = false;
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            RecordIterator<Entry> rowIter = new RecordIterator<Entry>() {
                boolean isClosed = false;

                @Override
                public boolean hasNext() throws StorageException {
                    ensureOpen();
                    return currentRow.hasNext();
                }

                @Override
                public Entry next() throws StorageException {
                    ensureOpen();
                    Map.Entry<Key, Value> kv = currentRow.next();

                    byte[] colQual = kv.getKey().getColumnQualifier().getBytes();
                    byte[] value = kv.getValue().get();
                    Entry entry = StaticBufferEntry.of(new StaticArrayBuffer(colQual), new StaticArrayBuffer(value));

                    return entry;
                }

                @Override
                public void close() throws StorageException {
                    isClosed = true;
                    currentRow = null;
                }

                private void ensureOpen() {
                    if (isClosed) {
                        throw new IllegalStateException("Iterator has been closed.");
                    }
                }
            };

            return rowIter;
        }

        @Override
        public boolean hasNext() throws StorageException {
            ensureOpen();
            return rows.hasNext();
        }

        @Override
        public StaticBuffer next() throws StorageException {
            ensureOpen();
            currentRow = Iterators.peekingIterator(rows.next());
            return new StaticArrayBuffer(currentRow.peek().getKey().getRow().getBytes());
        }

        @Override
        public void close() throws StorageException {
            isClosed = true;
            rows = null;
            currentRow = null;
        }

        private void ensureOpen() {
            if (isClosed) {
                throw new IllegalStateException("Iterator has been closed.");
            }
        }
    }
}
