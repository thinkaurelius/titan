package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

public class ExternalAstyanaxGraphPerformanceTest extends TitanGraphPerformanceTest {
	
	public ExternalAstyanaxGraphPerformanceTest() {
		super(StorageSetup.getAstyanaxGraphConfiguration(), 0, 1, false);
	}

}
