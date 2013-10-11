package com.thinkaurelius.titan.graphdb.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceMemoryTest;
import org.junit.BeforeClass;

import java.io.IOException;

public class AccumuloGraphPerformanceMemoryTest extends TitanGraphPerformanceMemoryTest {
    @BeforeClass
    public static void startAccumulo() throws IOException {
        AccumuloStorageSetup.startAccumulo();
    }

    public AccumuloGraphPerformanceMemoryTest() {
        super(AccumuloStorageSetup.getAccumuloGraphConfiguration());
    }
}
