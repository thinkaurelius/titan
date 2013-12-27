package com.thinkaurelius.titan.diskstorage.cassandra.astyanax;

import java.nio.ByteBuffer;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.ByteBufferSerializer;

import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnCounterStore;

@SuppressWarnings("unused")
public class CassandraAstyanaxCounterStore implements KeyColumnCounterStore {
    private final Keyspace keyspace;
    private final ColumnFamily<ByteBuffer, ByteBuffer> counter;

    public CassandraAstyanaxCounterStore(Keyspace keyspace, String columnFamily) {
        this.keyspace = keyspace;
        this.counter = new ColumnFamily<ByteBuffer, ByteBuffer>(columnFamily, ByteBufferSerializer.get(), ByteBufferSerializer.get());
    }

    @Override
    public void increment(StaticBuffer key, StaticBuffer column, long delta) throws StorageException {
        try {
            keyspace.prepareColumnMutation(counter, key.asByteBuffer(), column.asByteBuffer()).incrementCounterColumn(delta).execute();
        } catch (ConnectionException e) {
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public long get(StaticBuffer key, StaticBuffer column) throws StorageException {
        Column<ByteBuffer> result;
        try {
            result = keyspace.prepareQuery(counter).getKey(key.asByteBuffer()).getColumn(column.asByteBuffer())
                             .execute().getResult();
        } catch (ConnectionException e) {
            throw new PermanentStorageException(e);
        }

        return result == null ? 0 : result.getLongValue();
    }

    @Override
    public String getName() {
        return String.format("counter(%s, %s)", keyspace.getKeyspaceName(), counter.getName());
    }

    @Override
    public void close() throws StorageException {
       // Do nothing
    }
}
