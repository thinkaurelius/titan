package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

public class ExternalAstyanaxGraphPerformanceTest extends TitanGraphPerformanceTest {
	
	public ExternalAstyanaxGraphPerformanceTest() {
		super(CassandraStorageSetup.getAstyanaxGraphConfiguration(), 0, 1, false);
	}

}
