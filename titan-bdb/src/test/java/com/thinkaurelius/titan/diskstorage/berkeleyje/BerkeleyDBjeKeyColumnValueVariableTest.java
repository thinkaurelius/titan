package com.thinkaurelius.titan.diskstorage.berkeleyje;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.berkeleydb.je.BerkeleyJEStorageManager;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManagerAdapter;


public class BerkeleyDBjeKeyColumnValueVariableTest extends KeyColumnValueStoreTest {

    public StorageManager openStorageManager() {
        Configuration config = BerkeleyDBjeStorageSetup.getBerkeleyJEStorageConfiguration();
		BerkeleyJEStorageManager sm = new BerkeleyJEStorageManager(config);
		return new KeyValueStorageManagerAdapter(sm,config);
	}

}
