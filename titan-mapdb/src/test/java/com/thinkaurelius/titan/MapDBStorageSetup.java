package com.thinkaurelius.titan;

import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

public class MapDBStorageSetup extends StorageSetup {

    public static ModifiableConfiguration getMapDBConfiguration(String dir) {
        return buildConfiguration()
                .set(STORAGE_BACKEND, "mapdb")
                .set(STORAGE_DIRECTORY, dir);
    }

    public static ModifiableConfiguration getMapDBConfiguration() {
        return getMapDBConfiguration(getHomeDir());
    }

    public static WriteConfiguration getMapDBGraphConfiguration() {
        return getMapDBConfiguration().getConfiguration();
    }

}
