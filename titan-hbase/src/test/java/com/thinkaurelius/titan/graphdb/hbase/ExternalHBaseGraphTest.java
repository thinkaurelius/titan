package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.diskstorage.hbase.ExternalHBaseStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class ExternalHBaseGraphTest extends TitanGraphTest {

	public ExternalHBaseGraphTest() {
		super(ExternalHBaseStorageSetup.getHBaseGraphConfiguration());
	}

}
