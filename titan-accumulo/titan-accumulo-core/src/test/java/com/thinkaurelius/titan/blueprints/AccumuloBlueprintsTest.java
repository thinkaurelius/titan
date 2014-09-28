package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager;
import com.tinkerpop.blueprints.Graph;

import java.io.IOException;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class AccumuloBlueprintsTest extends TitanBlueprintsTest {

    @Override
    public void startUp() {
        try {
            AccumuloStorageSetup.startAccumulo();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public void shutDown() {
        // we don't need to restart on each test because cleanup is in please
    }

    @Override
    public Graph generateGraph() {
        return TitanFactory.open(AccumuloStorageSetup.getAccumuloGraphConfiguration());
    }

    @Override
    public void cleanUp() throws StorageException {
        AccumuloStoreManager s = AccumuloStorageSetup.getAccumuloStoreManager();
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
