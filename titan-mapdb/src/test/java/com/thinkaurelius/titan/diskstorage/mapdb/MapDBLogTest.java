package com.thinkaurelius.titan.diskstorage.mapdb;

import com.thinkaurelius.titan.MapDBStorageSetup;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import com.thinkaurelius.titan.diskstorage.log.KCVSLogTest;


public class MapDBLogTest extends KCVSLogTest {

    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        MapDBStoreManager sm = new MapDBStoreManager(MapDBStorageSetup.getMapDBConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

}
