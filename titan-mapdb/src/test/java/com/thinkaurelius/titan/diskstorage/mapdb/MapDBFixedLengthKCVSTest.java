package com.thinkaurelius.titan.diskstorage.mapdb;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.MapDBStorageSetup;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.junit.Test;


public class MapDBFixedLengthKCVSTest extends KeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        MapDBStoreManager sm = new MapDBStoreManager(MapDBStorageSetup.getMapDBConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(sm, ImmutableMap.of(storeName, 8));
    }

    @Test
    public void testGetKeysWithKeyRange() throws Exception {
        super.testGetKeysWithKeyRange();
    }
}
