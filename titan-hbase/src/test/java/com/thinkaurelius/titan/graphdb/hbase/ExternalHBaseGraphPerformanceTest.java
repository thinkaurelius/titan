package com.thinkaurelius.titan.graphdb.hbase;

import com.thinkaurelius.titan.diskstorage.hbase.ExternalHBaseStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

public class ExternalHBaseGraphPerformanceTest extends TitanGraphPerformanceTest {

	public ExternalHBaseGraphPerformanceTest() {
		super(ExternalHBaseStorageSetup.getHBaseGraphConfiguration(),0,1,false);
	}

    

}
