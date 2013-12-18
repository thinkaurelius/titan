package com.thinkaurelius.titan.diskstorage.keycolumnvalue;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 * A {@Link KeyColumnCounterStore} store counter values which can be incremented and retrieved.
 * This special store exists to allow backend implementations to implement incrementation operations
 * efficiently in distributed environments using dedicated counters instead of a get-then-set logic
 * which is prone to race-conditions.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface KeyColumnCounterStore {


    /**
     * Increments the value of the column counter of the specified row by the given delta.
     * Note, that delta might be negative in which case the value is decremented by delta's absolute value.
     *
     * @param key Row key
     * @param column Column
     * @param delta value by which to increment the column counter for the specified row
     * @throws StorageException
     */
    public void increment(StaticBuffer key, StaticBuffer column, long delta) throws StorageException;

    /**
     * Returns the value of the column counter of the specified row.
     * If this counter does (not yet) exist, 0 is returned.
     *
     * @param key Row key
     * @param column Column
     * @return value of the column counter of the specified row
     * @throws StorageException
     */
    public long get(StaticBuffer key, StaticBuffer column) throws StorageException;


    /**
     * Returns the name of this store. Each store has a unique name which is used to open it.
     *
     * @return store name
     * @see KeyColumnValueStoreManager#openDatabase(String)
     */
    public String getName();

    /**
     * Closes this store
     *
     * @throws com.thinkaurelius.titan.diskstorage.StorageException
     */
    public void close() throws StorageException;


}
