package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.FoundationDBTestSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.foundationdb.FoundationDBStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Graph;

public class FoundationDBBlueprintsTest extends TitanBlueprintsTest {

    @Override
    public void startUp() {

    }

    @Override
    public void shutDown() {
        // we don't need to restart on each test because cleanup is in please
    }

    @Override
    public Graph generateGraph() {
        return TitanFactory.open(FoundationDBTestSetup.getFoundationDBGraphConfig());
    }

    @Override
    public void cleanUp() throws StorageException {
        FoundationDBStoreManager s = new FoundationDBStoreManager(FoundationDBTestSetup
                .getFoundationDBGraphConfig()
                .subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE));
        s.clearStorage();
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return false;
    }

    @Override
    public Graph generateGraph(String s) {
        throw new UnsupportedOperationException();
    }
}
