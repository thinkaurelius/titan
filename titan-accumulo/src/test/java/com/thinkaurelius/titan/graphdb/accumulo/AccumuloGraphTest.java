package com.thinkaurelius.titan.graphdb.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import org.junit.BeforeClass;

import java.io.IOException;

public class AccumuloGraphTest extends TitanGraphTest {
    @BeforeClass
    public static void startAccumulo() throws IOException {
        AccumuloStorageSetup.startAccumulo();
    }

    public AccumuloGraphTest() {
        super(AccumuloStorageSetup.getAccumuloGraphConfiguration());
    }
}
