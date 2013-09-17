/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.diskstorage.StorageException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.commons.configuration.Configuration;

/**
 *
 * @author edeprit
 */
public class MockAccumuloStoreManager extends AccumuloStoreManager {
    
    static {
        instanceInjector = new AccumuloInstanceInjector() {
            @Override
            public Instance getInstance(String instanceName, String zooKeepers) {
                return new MockInstance(instanceName);
            }            
        };
    }
    
    public MockAccumuloStoreManager(Configuration config) throws StorageException {
        super(config);
    }
}
