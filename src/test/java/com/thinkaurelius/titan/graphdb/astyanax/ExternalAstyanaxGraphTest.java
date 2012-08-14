package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class ExternalAstyanaxGraphTest extends TitanGraphTest {

	public ExternalAstyanaxGraphTest() {
		super(StorageSetup.getAstyanaxGraphConfiguration());
	}
}
