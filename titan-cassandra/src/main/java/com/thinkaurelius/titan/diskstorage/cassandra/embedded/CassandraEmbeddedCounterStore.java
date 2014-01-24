package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import java.util.Collections;
import java.util.List;

import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnCounterStore;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnParent;

@SuppressWarnings("unused")
public class CassandraEmbeddedCounterStore implements KeyColumnCounterStore {
    private final String keyspace;
    private final String columnFamily;

    public CassandraEmbeddedCounterStore(String keyspace, String columnFamily) {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
    }

    @Override
    public void increment(StaticBuffer key, StaticBuffer column, long delta) throws StorageException {
        try {
            StorageProxy.mutate(Collections.singletonList(newCounterMutation(key, column, delta)), ConsistencyLevel.ONE);
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public long get(StaticBuffer key, StaticBuffer column) throws StorageException {
        List<Row> rows;

        try {
            rows = StorageProxy.read(Collections.singletonList(newCounterReadCommand(key, column)), ConsistencyLevel.ONE);
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        }

        assert rows != null && rows.size() == 1;

        Row row = rows.get(0);
        if (row.cf == null)
            return 0; // TODO: it depends if we want to return 0 or throw an error when column does not exist

        IColumn counter = row.cf.getColumn(column.asByteBuffer());
        return counter == null ? 0 : LongType.instance.compose(counter.value());
    }

    @Override
    public void clear(StaticBuffer key, StaticBuffer column) throws StorageException {
        try {
            StorageProxy.mutate(Collections.singletonList(newCounterRemoveMutation(key, column)), ConsistencyLevel.ONE);
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public String getName() {
        return String.format("counter(%s, %s)", keyspace, columnFamily);
    }

    @Override
    public void close() throws StorageException {
        // Nothing to cleanup
    }

    private CounterMutation newCounterMutation(StaticBuffer key, StaticBuffer column, long delta) {
        RowMutation rm = new RowMutation(keyspace, key.asByteBuffer());
        rm.addCounter(new QueryPath(columnFamily, null, column.asByteBuffer()), delta);
        return new CounterMutation(rm, ConsistencyLevel.ONE);
    }

    private CounterMutation newCounterRemoveMutation(StaticBuffer key, StaticBuffer column) {
        RowMutation rm = new RowMutation(keyspace, key.asByteBuffer());
        rm.delete(new QueryPath(columnFamily, null, column.asByteBuffer()), System.currentTimeMillis());
        return new CounterMutation(rm, ConsistencyLevel.ONE);
    }

    private ReadCommand newCounterReadCommand(StaticBuffer key, StaticBuffer column) {
        return new SliceByNamesReadCommand(keyspace,
                                           key.asByteBuffer(),
                                           new ColumnParent(columnFamily),
                                           Collections.singletonList(column.asByteBuffer()));
    }
}
