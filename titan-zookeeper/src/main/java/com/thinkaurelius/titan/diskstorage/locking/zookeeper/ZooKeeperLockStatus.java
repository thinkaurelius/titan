package com.thinkaurelius.titan.diskstorage.locking.zookeeper;

import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.diskstorage.locking.LockStatus;

public class ZooKeeperLockStatus implements LockStatus {
    private final long time;
    private final TimeUnit timeUnit;
    private final WriteLock writeLock;
    
    public ZooKeeperLockStatus(long time, TimeUnit timeUnit, WriteLock writeLock) {
        this.time = time;
        this.timeUnit = timeUnit;
        this.writeLock = writeLock;
    }
    
    @Override
    public long getExpirationTimestamp(TimeUnit tu) {
        return tu.convert(time, timeUnit);
    }
    
    public WriteLock getWritelock() {
        return writeLock;
    }
}
