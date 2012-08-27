package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyDBjeStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class BerkeleyJEGraphTest extends TitanGraphTest {

	public BerkeleyJEGraphTest() {
		super(BerkeleyDBjeStorageSetup.getBerkeleyJEGraphConfiguration());
	}

}
