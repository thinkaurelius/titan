package org.titan.cassandra.cql;

import static org.junit.Assert.*;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import com.thinkaurelius.titan.diskstorage.StorageException;

public class TestStorage {

	@Test
	public void testCreateStorage() throws StorageException {

		CqlStoreManager manager = new CqlStoreManager(
				getCassandraStorageConfiguration());
		CqlKeyColumnValueStore keystore = manager.openDatabase("titan");

		assertNotNull(keystore);
		manager.clearStorage();
		manager.close();

	}

	private Configuration getCassandraStorageConfiguration() {
		BaseConfiguration config = new BaseConfiguration();
		return config;

	}

}
