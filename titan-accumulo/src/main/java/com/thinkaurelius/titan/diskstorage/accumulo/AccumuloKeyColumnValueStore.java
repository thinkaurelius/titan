package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
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
    private static final Authorizations DEFAULT_AUTHORIZATIONS = new Authorizations();
    private static final Long DEFAULT_MAX_MEMORY = 50 * 1024 * 1024l;
    private static final Long DEFAULT_MAX_LATENCY = 100l;
    private static final Integer DEFAULT_MAX_QUERY_THREADS = 10;
    private static final Integer DEFAULT_MAX_WRITE_THREADS = 10;
    private final Connector connector;
    private final String tableName;
    private final String columnFamily;
    // This is columnFamily.getBytes()
    private final byte[] columnFamilyBytes;

    AccumuloKeyColumnValueStore(Connector connector, String tableName, String columnFamily) {
        this.connector = connector;
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.columnFamilyBytes = columnFamily.getBytes();
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
            logger.error("Should never throw this exception!", ex);
            throw new PermanentStorageException(ex);
        }

        byte[] keyBytes = key.as(StaticBuffer.ARRAY_FACTORY);
        scanner.setRange(new Range(new Text(keyBytes)));
        scanner.fetchColumnFamily(new Text(columnFamilyBytes));

        return scanner.iterator().hasNext();
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        Scanner scanner;
        try {
            scanner = connector.createScanner(tableName, DEFAULT_AUTHORIZATIONS);
        } catch (TableNotFoundException ex) {
            logger.error(ex.getMessage(), ex);
            throw new PermanentStorageException(ex);
        }

        byte[] keyBytes = query.getKey().as(StaticBuffer.ARRAY_FACTORY);
        Text keyText = new Text(keyBytes);
        Text cfText = new Text(columnFamilyBytes);

        byte[] startBytes = query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY);
        byte[] endBytes = query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY);

        Key startKey;
        Key endKey;

        if (startBytes.length > 0) {
            startKey = new Key(keyText, cfText, new Text(startBytes));
        } else {
            startKey = new Key(keyText, cfText);
        }

        if (endBytes.length > 0) {
            endKey = new Key(keyText, cfText, new Text(endBytes));
        } else {
            endKey = new Key(keyText, cfText);
        }

        scanner.setRange(new Range(startKey, true, endKey, false));
        if (query.getLimit() < scanner.getBatchSize()) {
            scanner.setBatchSize(query.getLimit());
        }

        int count = 0;
        List<Entry> entries = new ArrayList<Entry>();
        for (Map.Entry<Key, Value> entry : scanner) {
            byte[] cqBytes = entry.getKey().getColumnQualifier().getBytes();

            if (count < query.getLimit()) {
                entries.add(StaticBufferEntry.of(new StaticArrayBuffer(cqBytes),
                        new StaticArrayBuffer(entry.getValue().get())));
                count++;
            } else {
                break;
            }
        }

        return entries;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions,
            StoreTransaction txh) throws StorageException {

        List<Mutation> batch = makeBatch(columnFamilyBytes, key.as(StaticBuffer.ARRAY_FACTORY), additions, deletions);

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
                logger.error(ex.getMessage(), ex);
                throw new PermanentStorageException(ex);
            } finally {
                try {
                    writer.close();
                } catch (MutationsRejectedException ex) {
                    logger.warn(ex.getMessage(), ex);
                }
            }
        } catch (TableNotFoundException ex) {
            logger.error(ex.getMessage(), ex);
            throw new PermanentStorageException(ex);
        }
    }

    private static List<Mutation> makeBatch(byte[] cfName, byte[] key, List<Entry> additions, List<StaticBuffer> deletions) {
        if (additions.isEmpty() && deletions.isEmpty()) {
            return Collections.emptyList();
        }

        List<Mutation> batch = new ArrayList<Mutation>(2);

        if (!additions.isEmpty()) {
            Mutation put = makePutMutation(cfName, key, additions);
            batch.add(put);
        }

        if (!deletions.isEmpty()) {
            Mutation delete = makeDeleteMutation(cfName, key, deletions);
            batch.add(delete);
        }
        return batch;
    }

    /**
     * Convert deletions to a Delete mutation.
     *
     * @param cfName The name of the ColumnFamily deletions belong to
     * @param key The row key
     * @param deletions The name of the columns to delete (a.k.a deletions)
     *
     * @return Delete command or null if deletions were null or empty.
     */
    private static Mutation makeDeleteMutation(byte[] cfName, byte[] key, List<StaticBuffer> deletions) {
        Preconditions.checkArgument(!deletions.isEmpty());

        Mutation mutation = new Mutation(new Text(key));
        for (StaticBuffer del : deletions) {
            byte[] cqName = del.as(StaticBuffer.ARRAY_FACTORY);
            mutation.putDelete(new Text(cfName), new Text(cqName));
        }
        return mutation;
    }

    /**
     * Convert modification entries into Put command.
     *
     * @param cfName The name of the ColumnFamily modifications belong to
     * @param key The row key
     * @param modifications The entries to insert/update.
     *
     * @return Put command or null if additions were null or empty.
     */
    private static Mutation makePutMutation(byte[] cfName, byte[] key, List<Entry> modifications) {
        Preconditions.checkArgument(!modifications.isEmpty());

        Mutation mutation = new Mutation(new Text(key));
        for (Entry entry : modifications) {
            byte[] cqName = entry.getArrayColumn();
            byte[] value = entry.getArrayValue();
            mutation.put(new Text(cfName), new Text(cqName), new Value(value));
        }
        return mutation;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue,
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
        final BatchScanner scanner;

        try {
            scanner = connector.createBatchScanner(tableName,
                    DEFAULT_AUTHORIZATIONS, DEFAULT_MAX_QUERY_THREADS);
        } catch (TableNotFoundException ex) {
            logger.error(ex.getMessage(), ex);
            throw new PermanentStorageException(ex);
        }

        scanner.setRanges(Collections.singletonList(new Range()));
        
        scanner.fetchColumnFamily(new Text(columnFamily));

        IteratorSetting firstKeyOnlyIterator = new IteratorSetting(10, "keyIter", FirstEntryInRowIterator.class);
        scanner.addScanIterator(firstKeyOnlyIterator);

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
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return columnFamily;
    }
}
