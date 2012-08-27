package com.thinkaurelius.titan.graphdb.cassandra;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraStorageSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphPerformanceTest;

public class ExternalCassandraGraphPerformanceTest extends TitanGraphPerformanceTest {


    public static CassandraProcessStarter ch = new CassandraProcessStarter();

    public ExternalCassandraGraphPerformanceTest() {
        super(CassandraStorageSetup.getCassandraGraphConfiguration(),0,1,false);
    }

    @BeforeClass
    public static void beforeClass() {
        ch.startCassandra();
    }

    @AfterClass
    public static void afterClass() throws InterruptedException {
        ch.stopCassandra();
    }

}
