package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.thinkaurelius.titan.FoundationDBTestSetup;
import com.thinkaurelius.titan.diskstorage.KeyValueStoreTest;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;

public class FoundationDBKeyValueTest extends KeyValueStoreTest {

    @Override
    public OrderedKeyValueStoreManager openStorageManager() throws BackendException {
        return new FoundationDBStoreManager(FoundationDBTestSetup.getFoundationDBConfig());
    }
}
