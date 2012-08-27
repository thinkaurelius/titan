package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyDBjeStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class BerkeleyJEGraphConcurrentTest extends TitanGraphConcurrentTest {

	public BerkeleyJEGraphConcurrentTest() {
		super(BerkeleyDBjeStorageSetup.getBerkeleyJEGraphConfiguration());
	}

}
