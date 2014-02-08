    package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.thinkaurelius.titan.BerkeleyJeStorageSetup;
import com.thinkaurelius.titan.diskstorage.HashKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;

public class BerkeleyJeHashKeyColumnValueTest extends HashKeyColumnValueStoreTest {

    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return new OrderedKeyValueStoreManagerAdapter(
                       new BerkeleyJEStoreManager(BerkeleyJeStorageSetup.getBerkeleyJEConfiguration()));

//        BerkeleyJEStoreManager sm = new BerkeleyJEStoreManager(BerkeleyJeStorageSetup.getBerkeleyJEConfiguration());
//
//        // prefixed store doesn't support scan, because prefix is hash of a key which makes it un-ordered
//        sm.features.supportsUnorderedScan = false;
//        sm.features.supportsOrderedScan = false;
//
//        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

//    @Test
//    @Override
//    public void testGetKeysWithKeyRange() {
//        // Requires ordered keys, but we are using hash prefix
//    }
//
//    @Test
//    @Override
//    public void testOrderedGetKeysRespectsKeyLimit() {
//        // Requires ordered keys, but we are using hash prefix
//    }
}