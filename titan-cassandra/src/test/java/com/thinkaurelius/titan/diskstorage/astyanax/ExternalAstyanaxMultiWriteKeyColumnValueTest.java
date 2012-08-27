package com.thinkaurelius.titan.diskstorage.astyanax;

import com.thinkaurelius.titan.diskstorage.MultiWriteKeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraStorageSetup;

public class ExternalAstyanaxMultiWriteKeyColumnValueTest extends MultiWriteKeyColumnValueStoreTest {

    @Override
    public StorageManager openStorageManager() {
        return new AstyanaxStorageManager(CassandraStorageSetup.getCassandraStorageConfiguration());
    }
}
