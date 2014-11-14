package com.thinkaurelius.titan.graphdb.foundationdb;

import com.thinkaurelius.titan.FoundationDBTestSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphConcurrentTest;

public class FoundationDBGraphConcurrentTest extends TitanGraphConcurrentTest{

    @Override
    public WriteConfiguration getConfiguration() {
        return FoundationDBTestSetup.getFoundationDBGraphConfig();
    }

}
