package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.diskstorage.hbase.ExternalHBaseStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class ExternalHBaseGraphConcurrentTest extends TitanGraphConcurrentTest {

	public ExternalHBaseGraphConcurrentTest() {
		super(ExternalHBaseStorageSetup.getHBaseGraphConfiguration());
	}

    

}
