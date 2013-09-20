package com.thinkaurelius.titan.graphdb.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;
import org.junit.BeforeClass;

import java.io.IOException;

public class AccumuloGraphConcurrentTest extends TitanGraphConcurrentTest {
    @BeforeClass
    public static void startAccumulo() throws IOException {
        AccumuloStorageSetup.startAccumulo();
    }

    public AccumuloGraphConcurrentTest() {
        super(AccumuloStorageSetup.getAccumuloGraphConfiguration());
    }
}
