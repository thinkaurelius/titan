/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import java.util.Collections;
import org.apache.commons.configuration.BaseConfiguration;

/**
 *
 * @author edeprit
 */
public class TestAccumulo {

    public static void main(String[] args) throws Exception {
        KeyColumnValueStoreManager manager = new AccumuloStoreManager(new BaseConfiguration());
        KeyColumnValueStore store = manager.openDatabase("foo");

        StoreTransaction txh = manager.beginTransaction(ConsistencyLevel.DEFAULT);

        Entry entry = new StaticBufferEntry(ByteBufferUtil.getIntBuffer(2), ByteBufferUtil.getIntBuffer(3));
        
        StaticBuffer key = ByteBufferUtil.getIntBuffer(1);

        store.mutate(key, Collections.singletonList(entry),
                KeyColumnValueStore.NO_DELETIONS, txh);
        
        KeySliceQuery query = new KeySliceQuery(key, new StaticArrayBuffer(new byte[0]), new StaticArrayBuffer(new byte[0]));
        for (Entry e : store.getSlice(query, txh)) {
            System.out.println(e);
        }
    }
}
