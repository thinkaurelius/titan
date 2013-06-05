package com.thinkaurelius.titan.graphdb.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;
import org.junit.BeforeClass;

import java.io.IOException;

public class AccumuloGraphPerformanceTest extends TitanGraphPerformanceTest {
    @BeforeClass
    public static void startAccumulo() throws IOException {
        AccumuloStorageSetup.startAccumulo();
    }

    public AccumuloGraphPerformanceTest() {
        super(AccumuloStorageSetup.getAccumuloGraphConfiguration(), 0, 1, false);
    }
}
