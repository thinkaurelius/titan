package com.thinkaurelius.titan.blueprints;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;

/**
 * Created by bryn on 06/05/15.
 */
public class InMemoryGraphProvider extends AbstractTitanGraphProvider {
    @Override
    public ModifiableConfiguration getTitanConfiguration(String graphName, Class<?> test, String testMethodName) {
        return StorageSetup.getInMemoryConfiguration();
    }
}
