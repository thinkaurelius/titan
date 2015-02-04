package com.thinkaurelius.titan.graphdb.mapdb;

import com.thinkaurelius.titan.MapDBStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.olap.FulgoraOLAPTest;

public class MapDBOLAPTest extends FulgoraOLAPTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return MapDBStorageSetup.getMapDBGraphConfiguration();
    }

}
