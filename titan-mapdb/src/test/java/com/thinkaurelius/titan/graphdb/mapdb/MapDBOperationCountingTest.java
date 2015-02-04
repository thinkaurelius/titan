package com.thinkaurelius.titan.graphdb.mapdb;

import com.thinkaurelius.titan.MapDBStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanOperationCountingTest;

public class MapDBOperationCountingTest extends TitanOperationCountingTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return MapDBStorageSetup.getMapDBGraphConfiguration();
    }

}
