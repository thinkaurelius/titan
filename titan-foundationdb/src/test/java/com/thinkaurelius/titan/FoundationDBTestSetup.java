package com.thinkaurelius.titan;

import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

public class FoundationDBTestSetup extends StorageSetup {

    public static ModifiableConfiguration getFoundationDBConfig() {
        return buildConfiguration()
            .set(STORAGE_BACKEND, "foundationdb");
    }

    public static WriteConfiguration getFoundationDBGraphConfig() {
        return getFoundationDBConfig().getConfiguration();
    }

}
