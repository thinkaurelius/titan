package com.thinkaurelius.titan.diskstorage.accumulo;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;

/**
 * Instance injector for Accumulo store configuration.
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public interface AccumuloInstanceInjector {

    public Instance getInstance(String instanceName, String zooKeepers);
    //
    public static final AccumuloInstanceInjector ZOOKEEPER_INSTANCE_INJECTOR =
            new AccumuloInstanceInjector() {
        @Override
        public Instance getInstance(String instanceName, String zooKeepers) {
            return new ZooKeeperInstance(instanceName, zooKeepers);
        }
    };
}
