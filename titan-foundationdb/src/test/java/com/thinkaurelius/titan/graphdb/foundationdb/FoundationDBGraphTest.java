package com.thinkaurelius.titan.graphdb.foundationdb;

import com.thinkaurelius.titan.FoundationDBTestSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;

public class FoundationDBGraphTest extends TitanGraphTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return FoundationDBTestSetup.getFoundationDBGraphConfig();
    }

    @Override
    protected boolean isLockingOptimistic() {
        return true;
    }
}
