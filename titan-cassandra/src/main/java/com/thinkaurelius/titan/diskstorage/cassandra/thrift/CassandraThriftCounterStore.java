package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import java.nio.ByteBuffer;
import java.util.Collections;

import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnectionPool;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnCounterStore;

import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.thrift.*;

@SuppressWarnings("unused")
public class CassandraThriftCounterStore implements KeyColumnCounterStore {
    private final String keyspace;
    private final String columnFamily;
    private final CTConnectionPool pool;

    public CassandraThriftCounterStore(String keyspace, String columnFamily, CTConnectionPool pool) {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.pool = pool;
    }

    @Override
    public void increment(StaticBuffer key, StaticBuffer column, long delta) throws StorageException {
        CTConnection conn = null;
        try {
            conn = pool.borrowObject(keyspace);
            conn.getClient().add(key.asByteBuffer(), new ColumnParent(columnFamily), new CounterColumn(column.asByteBuffer(), delta), ConsistencyLevel.ONE);
        } catch (Exception e) {
            throw new TemporaryStorageException(e);
        } finally {
            pool.returnObjectUnsafe(keyspace, conn);
        }
    }

    @Override
    public long get(StaticBuffer key, StaticBuffer column) throws StorageException {
        CTConnection conn = null;
        try {
            conn = pool.borrowObject(keyspace);
            ColumnOrSuperColumn counter = conn.getClient().get(key.asByteBuffer(),
                                                  new ColumnPath(columnFamily).setColumn(column.asByteBuffer()),
                                                  ConsistencyLevel.ONE);

            return counter == null ? 0L : counter.getCounter_column().getValue();
        } catch (NotFoundException e) {
            return 0L;
        } catch (Exception e) {
            throw new TemporaryStorageException(e);
        } finally {
            pool.returnObjectUnsafe(keyspace, conn);
        }
    }

    @Override
    public void clear(StaticBuffer key, StaticBuffer column) throws StorageException {
        CTConnection conn = null;
        try {
            conn = pool.borrowObject(keyspace);
            conn.getClient().remove_counter(key.asByteBuffer(),
                                            new ColumnPath(columnFamily).setColumn(column.asByteBuffer()),
                                            ConsistencyLevel.ONE);
        } catch (Exception e) {
            throw new TemporaryStorageException(e);
        } finally {
            pool.returnObjectUnsafe(keyspace, conn);
        }
    }

    @Override
    public String getName() {
        return String.format("counter(%s, %s)", keyspace, columnFamily);
    }

    @Override
    public void close() throws StorageException {
        try {
            pool.close();
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        }
    }
}
