package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnCounterStore;
import com.thinkaurelius.titan.util.system.IOUtils;

import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class HBaseCounterStore implements KeyColumnCounterStore {
    private static final Logger logger = LoggerFactory.getLogger(HBaseCounterStore.class);

    private final String tableName;
    private final byte[] columnFamilyBytes;
    private final HTablePool pool;

    private final HBaseStoreManager storeManager;

    HBaseCounterStore(HBaseStoreManager storeManager,  HTablePool pool, String tableName, String columnFamily) {
        this.storeManager = storeManager;
        this.tableName = tableName;
        this.pool = pool;
        this.columnFamilyBytes = columnFamily.getBytes();
    }

    @Override
    public void increment(StaticBuffer key, StaticBuffer column, long delta) throws StorageException {
        HTableInterface table = null;

        try {
            table = pool.getTable(tableName);
            table.incrementColumnValue(toBytes(key), columnFamilyBytes, toBytes(column), delta);
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    public long get(StaticBuffer key, StaticBuffer column) throws StorageException {
        HTableInterface table = null;
        byte[] columnName = toBytes(column);

        try {
            table = pool.getTable(tableName);
            Result count = table.get(new Get(toBytes(key)).addColumn(columnFamilyBytes, columnName));

            if (count.isEmpty())
                return 0;

            return Bytes.toLong(count.getValue(columnFamilyBytes, columnName));
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    public void clear(StaticBuffer key, StaticBuffer column) throws StorageException {
        HTableInterface table = null;

        try {
            table = pool.getTable(tableName);
            table.delete(new Delete(toBytes(key)).deleteColumn(columnFamilyBytes, toBytes(column)));
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    public String getName() {
        return String.format("HBase:counter(%s, %s)", tableName, new String(columnFamilyBytes));
    }

    @Override
    public void close() throws StorageException {
        try {
            pool.closeTablePool(tableName);
        } catch (IOException e) {
            logger.error("Non fatal error while trying to close " + tableName + ".", e);
        }
    }

    private static byte[] toBytes(StaticBuffer buffer) {
        return buffer.as(StaticBuffer.ARRAY_FACTORY);
    }
}
