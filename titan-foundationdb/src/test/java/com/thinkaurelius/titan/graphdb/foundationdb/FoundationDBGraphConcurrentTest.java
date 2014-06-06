package com.thinkaurelius.titan.graphdb.foundationdb;

import com.thinkaurelius.titan.FoundationDBTestSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class FoundationDBGraphConcurrentTest extends TitanGraphConcurrentTest{

    public FoundationDBGraphConcurrentTest() {
        super(FoundationDBTestSetup.getFoundationDBGraphConfig());
    }
}
