package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import com.thinkaurelius.titan.diskstorage.accumulo.iterators.ColumnRangeFilter;
import com.thinkaurelius.titan.diskstorage.accumulo.util.CallableFunction;
import com.thinkaurelius.titan.diskstorage.accumulo.util.ConcurrentLists;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

/**
 * Key-Column value store for Accumulo.
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public class AccumuloKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloKeyColumnValueStore.class);
    // Default parameters 
    private static final int NUM_THREADS_DEFAULT = Runtime.getRuntime().availableProcessors();
    private static final Authorizations AUTHORIZATIONS_DEFAULT = new Authorizations();
    // Instance variables
    private final Connector connector;  // thread-safe
    private final String tableName;
    private final String columnFamily;
    private final byte[] columnFamilyBytes;
    private final Text columnFamilyText;
    private final AccumuloBatchConfiguration batchConfiguration;
    private final boolean serverSideIterators;

    AccumuloKeyColumnValueStore(Connector connector, String tableName, String columnFamily,
            AccumuloBatchConfiguration batchConfiguration, boolean serverSideIterators) {
        this.connector = connector;
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.columnFamilyBytes = columnFamily.getBytes();
        this.columnFamilyText = new Text(columnFamily);
        this.batchConfiguration = batchConfiguration;
        this.serverSideIterators = serverSideIterators;
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
            scanner = connector.createScanner(tableName, AUTHORIZATIONS_DEFAULT);
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
            scanner = connector.createScanner(tableName, AUTHORIZATIONS_DEFAULT);
        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }

        scanner.setRange(getRange(query));
        if (query.getLimit() < scanner.getBatchSize()) {
            scanner.setBatchSize(query.getLimit());
        }

        int count = 1;
        List<Entry> slice = new ArrayList<Entry>();
        for (Map.Entry<Key, Value> kv : scanner) {
            if (count > query.getLimit()) {
                break;
            }
            slice.add(getEntry(kv));
            count++;
        }

        return slice;
    }

    @Override
    public List<List<Entry>> getSlice(List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws StorageException {
        List<List<Entry>> slices = ConcurrentLists.transform(keys,
                new CallableFunction<StaticBuffer, List<Entry>>() {
            @Override
            public List<Entry> apply(StaticBuffer key) throws Exception {
                return getSlice(new KeySliceQuery(key, query), txh);
            }
        });

        return slices;
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
            BatchWriter writer = batchConfiguration.createBatchWriter(connector, tableName);
            try {
                writer.addMutations(batch);
                writer.flush();
            } catch (MutationsRejectedException ex) {
                logger.error("Can't write mutations to Titan store " + tableName, ex);
                throw new TemporaryStorageException(ex);
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
     * Convert Titan deletions into Accumulo delete {@code Mutation}.
     *
     * @param colFamily Name of column family for deletions
     * @param key Row key
     * @param deletions Name of column qualifiers to delete
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
     * Convert Titan {@code Entry} modifications entries into Accumulo put {@code Mutation}.
     *
     * @param colFamily Name of column family for modifications
     * @param key Row key
     * @param modifications Entries to insert/update.
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
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws StorageException {
        return executeKeySliceQuery(query.getKeyStart(), query.getKeyEnd(), query, false);
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException {
        return executeKeySliceQuery(query, false);
    }

    private KeyIterator executeKeySliceQuery(SliceQuery columnSlice, boolean keysOnly) throws StorageException {
        return executeKeySliceQuery(null, null, columnSlice, keysOnly);
    }

    private KeyIterator executeKeySliceQuery(StaticBuffer startKey, StaticBuffer endKey,
            SliceQuery columnSlice, boolean keysOnly) throws StorageException {

        Scanner scanner = getKeySliceScanner(startKey, endKey, columnSlice, keysOnly);

        return new RowKeyIterator(scanner);
    }

    private Scanner getKeySliceScanner(StaticBuffer startKey, StaticBuffer endKey,
            SliceQuery columnSlice, boolean keysOnly) throws StorageException {

        Scanner scanner;
        try {
            scanner = connector.createScanner(tableName, AUTHORIZATIONS_DEFAULT);

        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }

        Range range = getRange(startKey, endKey);
        scanner.setRange(range);
        scanner.fetchColumnFamily(columnFamilyText);

        IteratorSetting columnSliceIterator = null;
        if (columnSlice != null) {
            columnSliceIterator = getColumnSliceIterator(columnSlice);

            if (columnSliceIterator != null) {
                if (!serverSideIterators) {
                    scanner = new ClientSideIteratorScanner(scanner);
                }
                scanner.addScanIterator(columnSliceIterator);
            }
        }

        if (keysOnly) {
            IteratorSetting firstRowKeyIterator = new IteratorSetting(15, "firstRowKeyIter", FirstEntryInRowIterator.class);
            scanner.addScanIterator(firstRowKeyIterator);
        }

        return scanner;
    }

    private static Entry getEntry(Map.Entry<Key, Value> keyValue) {
        byte[] colQual = keyValue.getKey().getColumnQualifier().getBytes();
        byte[] value = keyValue.getValue().get();
        return StaticBufferEntry.of(new StaticArrayBuffer(colQual), new StaticArrayBuffer(value));
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

    private Range getRange(KeySliceQuery query) {
        return getRange(query.getKey(), query);
    }

    private Range getRange(StaticBuffer key, SliceQuery query) {
        Text row = new Text(key.as(StaticBuffer.ARRAY_FACTORY));

        Key startKey;
        Key endKey;

        if (query.getSliceStart().length() > 0) {
            startKey = new Key(row, columnFamilyText,
                    new Text(query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY)));
        } else {
            startKey = new Key(row, columnFamilyText);
        }

        if (query.getSliceEnd().length() > 0) {
            endKey = new Key(row, columnFamilyText,
                    new Text(query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY)));
        } else {
            endKey = new Key(row, columnFamilyText);
        }

        return new Range(startKey, true, endKey, false);
    }

    public static IteratorSetting getColumnSliceIterator(SliceQuery sliceQuery) {
        IteratorSetting is = null;

        byte[] minColumn = sliceQuery.getSliceStart().as(StaticBuffer.ARRAY_FACTORY);
        byte[] maxColumn = sliceQuery.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY);

        if (minColumn.length > 0 || maxColumn.length > 0) {
            is = new IteratorSetting(10, "columnRangeIter", ColumnRangeFilter.class);
            ColumnRangeFilter.setRange(is, minColumn, true, maxColumn, false);
        }

        return is;
    }

    private static class RowKeyIterator implements KeyIterator {

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
                @Override
                public boolean hasNext() {
                    ensureOpen();
                    return currentRow.hasNext();
                }

                @Override
                public Entry next() {
                    ensureOpen();
                    Map.Entry<Key, Value> kv = currentRow.next();
                    return getEntry(kv);
                }

                @Override
                public void close() {
                    isClosed = true; // same semantics as in-memory implementation in Titan core
                }
                
                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };

            return rowIter;
        }

        @Override
        public boolean hasNext() {
            ensureOpen();
            return rows.hasNext();
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();
            currentRow = Iterators.peekingIterator(rows.next());
            return new StaticArrayBuffer(currentRow.peek().getKey().getRow().getBytes());
        }

        @Override
        public void close() {
            isClosed = true;
            rows = null;
            currentRow = null;
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void ensureOpen() {
            if (isClosed) {
                throw new IllegalStateException("Iterator has been closed.");
            }
        }
    }
}
