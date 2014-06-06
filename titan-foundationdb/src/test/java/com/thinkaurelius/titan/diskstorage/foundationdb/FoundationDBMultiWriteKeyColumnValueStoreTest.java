package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.thinkaurelius.titan.FoundationDBTestSetup;
import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;

public class FoundationDBMultiWriteKeyColumnValueStoreTest extends MultiWriteKeyColumnValueStoreTest {
    @Override
    public KeyColumnValueStoreManager openStorageManager() {
        OrderedKeyValueStoreManager osm = new FoundationDBStoreManager(FoundationDBTestSetup.getFoundationDBConfig());
        return new OrderedKeyValueStoreManagerAdapter(osm);
    }
}
