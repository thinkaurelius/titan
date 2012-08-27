package com.thinkaurelius.titan.graphdb.cassandra;

import org.junit.BeforeClass;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraDaemonWrapper;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class InternalCassandraGraphTest extends TitanGraphTest {

	public InternalCassandraGraphTest() {
		super(CassandraStorageSetup.getCassandraGraphConfiguration());
	}

	@BeforeClass
	public static void beforeClass() {
		CassandraDaemonWrapper.start(CassandraStorageSetup.cassandraYamlPath);
	}
}
