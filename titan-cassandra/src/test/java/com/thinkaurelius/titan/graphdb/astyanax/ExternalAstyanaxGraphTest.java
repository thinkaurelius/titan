package com.thinkaurelius.titan.graphdb.astyanax;

import com.thinkaurelius.titan.CassandraStorageSetup;
import com.thinkaurelius.titan.diskstorage.cassandra.CassandraProcessStarter;
import com.thinkaurelius.titan.graphdb.AbstractCassandraGraphTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ExternalAstyanaxGraphTest extends AbstractCassandraGraphTest {

    public ExternalAstyanaxGraphTest() {
        super(CassandraStorageSetup.getAstyanaxGraphConfiguration(ExternalAstyanaxGraphTest.class.getSimpleName()));
    }

    public static CassandraProcessStarter ch = new CassandraProcessStarter();

    @BeforeClass
    public static void startCassandra() {
        ch.startCassandra();
    }

    @AfterClass
    public static void stopCassandra() {
        ch.stopCassandra();
    }
}
