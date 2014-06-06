package com.thinkaurelius.titan.graphdb.foundationdb;

import com.thinkaurelius.titan.FoundationDBTestSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;

public class FoundationDBGraphPerformanceTest extends TitanGraphPerformanceMemoryTest {
    public FoundationDBGraphPerformanceTest() {
        super(FoundationDBTestSetup.getFoundationDBGraphConfig());
    }
}
