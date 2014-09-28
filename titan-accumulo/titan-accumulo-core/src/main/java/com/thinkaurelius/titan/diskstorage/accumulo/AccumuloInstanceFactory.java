package com.thinkaurelius.titan.diskstorage.accumulo;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;

/**
 * Instance factory for Accumulo store configuration.
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public interface AccumuloInstanceFactory {

    public Instance getInstance(String instanceName, String zooKeepers);
    
    /*
     * Default Zookeeper instance factory.
     */
    public static final AccumuloInstanceFactory ZOOKEEPER_INSTANCE_FACTORY =
            new AccumuloInstanceFactory() {
        @Override
        public Instance getInstance(String instanceName, String zooKeepers) {
            return new ZooKeeperInstance(instanceName, zooKeepers);
        }
    };
}
