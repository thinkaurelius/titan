package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class ExternalAstyanaxGraphTest extends TitanGraphTest {

	public ExternalAstyanaxGraphTest() {
		super(CassandraStorageSetup.getAstyanaxGraphConfiguration());
	}
}
