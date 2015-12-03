package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class HBaseMultiQueryGraphProvider extends HBaseGraphProvider {

    @Override
    public ModifiableConfiguration getTitanConfiguration(String graphName, Class<?> test, String testMethodName) {
        return super.getTitanConfiguration(graphName, test, testMethodName).
                set(GraphDatabaseConfiguration.USE_MULTIQUERY, true);
    }
}
