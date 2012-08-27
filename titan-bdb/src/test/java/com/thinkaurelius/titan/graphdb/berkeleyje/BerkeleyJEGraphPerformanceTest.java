package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyDBjeStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

public class BerkeleyJEGraphPerformanceTest extends TitanGraphPerformanceTest {

	public BerkeleyJEGraphPerformanceTest() {
		super(BerkeleyDBjeStorageSetup.getBerkeleyJEGraphConfiguration(),0,1,false);
	}

}
