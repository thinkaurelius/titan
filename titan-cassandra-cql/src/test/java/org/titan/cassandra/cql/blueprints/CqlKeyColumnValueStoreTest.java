package org.titan.cassandra.cql.blueprints;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND_KEY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NAMESPACE;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.titan.cassandra.cql.CqlStoreManager;

import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

public class CqlKeyColumnValueStoreTest extends KeyColumnValueStoreTest{


	@Override
	public KeyColumnValueStoreManager openStorageManager()
			throws StorageException {
		Configuration config = new BaseConfiguration();
		config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_BACKEND_KEY,
				"cassandra-cql");
		
		return new CqlStoreManager(config);
	}

}
