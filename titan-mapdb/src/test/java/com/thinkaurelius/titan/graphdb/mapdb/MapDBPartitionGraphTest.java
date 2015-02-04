package com.thinkaurelius.titan.graphdb.mapdb;

import com.thinkaurelius.titan.MapDBStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanPartitionGraphTest;
import org.junit.Ignore;


/**
 * TODO: debug berkeley dbs keyslice method
 */
@Ignore
public class MapDBPartitionGraphTest extends TitanPartitionGraphTest {

    @Override
    public WriteConfiguration getBaseConfiguration() {
        return MapDBStorageSetup.getMapDBGraphConfiguration();
    }

}
