package com.thinkaurelius.titan.diskstorage.locking.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Does nothing with events except log them.
 */
public class NoopWatcher implements Watcher {
    
    private static final Logger log =
            LoggerFactory.getLogger(NoopWatcher.class);

    @Override
    public void process(WatchedEvent event) {
        log.trace("Ignoring event [{}]", event);
    }

}
