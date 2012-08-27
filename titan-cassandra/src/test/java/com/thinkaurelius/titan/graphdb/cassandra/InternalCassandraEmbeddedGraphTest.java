package com.thinkaurelius.titan.graphdb.cassandra;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class InternalCassandraEmbeddedGraphTest extends TitanGraphTest {

	public InternalCassandraEmbeddedGraphTest() {
		super(CassandraStorageSetup.getEmbeddedCassandraGraphConfiguration());
	}
}
