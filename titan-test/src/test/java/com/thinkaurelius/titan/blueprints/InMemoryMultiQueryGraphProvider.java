package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Created by twilmes on 06/05/15.
 */
public class InMemoryMultiQueryGraphProvider extends InMemoryGraphProvider {
    @Override
    public ModifiableConfiguration getTitanConfiguration(String graphName, Class<?> test, String testMethodName) {
        return super.getTitanConfiguration(graphName, test, testMethodName).
                set(GraphDatabaseConfiguration.USE_MULTIQUERY, true);
    }
}
