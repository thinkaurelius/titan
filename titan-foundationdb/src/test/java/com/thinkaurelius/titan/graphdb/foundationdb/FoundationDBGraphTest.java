package com.thinkaurelius.titan.graphdb.foundationdb;

import com.thinkaurelius.titan.FoundationDBTestSetup;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

public class FoundationDBGraphTest extends TitanGraphTest {

    public FoundationDBGraphTest() {
        super(FoundationDBTestSetup.getFoundationDBGraphConfig());
    }
}
