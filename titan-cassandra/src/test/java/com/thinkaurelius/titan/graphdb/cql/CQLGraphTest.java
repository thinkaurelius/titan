package com.thinkaurelius.titan.graphdb.cql;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

import org.junit.BeforeClass;

public class CQLGraphTest extends TitanGraphTest {
    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded(CassandraStorageSetup.YAML_PATH);
    }

	@Override
	public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getCassandraCQLGraphConfiguration(getClass().getSimpleName());
	}
}
