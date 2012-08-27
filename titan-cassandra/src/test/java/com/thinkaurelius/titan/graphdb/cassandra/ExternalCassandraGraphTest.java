package com.thinkaurelius.titan.graphdb.cassandra;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class ExternalCassandraGraphTest extends TitanGraphTest {

	public static CassandraProcessStarter ch = new CassandraProcessStarter();
	
	public ExternalCassandraGraphTest() {
		super(CassandraStorageSetup.getCassandraGraphConfiguration());
	}

	@BeforeClass
	public static void beforeClass() {
		ch.startCassandra();
	}
	
	@AfterClass
	public static void afterClass() throws InterruptedException {
		ch.stopCassandra();
	}

}
