package com.thinkaurelius.titan.diskstorage.locking.zookeeper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.AbstractLocker;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediators;
import com.thinkaurelius.titan.diskstorage.locking.Locker;
import com.thinkaurelius.titan.diskstorage.locking.LockerState;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockerSerializer;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;

public class ZooKeeperLocker extends AbstractLocker<ZooKeeperLockStatus> implements Locker {

    private final ZooKeeper zk;
    private final String dir;
    
    public static class Builder extends AbstractLocker.Builder<ZooKeeperLockStatus, Builder> {

        private String zkdir;
        private String hostname;
        private int port;
        
        @Override
        protected Builder self() {
            return this;
        }
     
        @Override
        protected LocalLockMediator<StoreTransaction> getDefaultMediator() {
            return LocalLockMediators.INSTANCE.get("zookeeper:" + zkdir);
        }
        
        public ZooKeeperLocker build() {
            super.preBuild();
            return new ZooKeeperLocker(rid, times, serializer, llm, lockState, lockExpireNS, log, hostname, port, zkdir);
        }
    }
    
    public ZooKeeperLocker(StaticBuffer rid, TimestampProvider times,
            ConsistentKeyLockerSerializer serializer,
            LocalLockMediator<StoreTransaction> llm,
            LockerState<ZooKeeperLockStatus> lockState, long lockExpireNS,
            Logger log, String hostname, int port, String zkdir) {
        super(rid, times, serializer, llm, lockState, lockExpireNS, log);
        try {
            zk = new ZooKeeper(hostname, port, new NoopWatcher());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        dir = zkdir;
    }
    
    private String getLockDir(KeyColumn lockID) {
        return dir + "/" + lockID.getKey().toString() + "/" + lockID.getColumn();
    }

    @Override
    protected ZooKeeperLockStatus writeSingleLock(KeyColumn lockID, StoreTransaction tx)
            throws Throwable {
        String lockDir = getLockDir(lockID);
        WriteLock wl = new WriteLock(zk, lockDir, null, TimeUnit.MILLISECONDS.convert(lockExpireNS, TimeUnit.NANOSECONDS));
        
        if (!wl.lock()) {
            throw new TemporaryLockingException("ZK lock attempt returned false");
        }
        log.debug("Locked {} in ZK", lockID);

        long approxTimeNS = times.getApproxNSSinceEpoch(false);
        return new ZooKeeperLockStatus(approxTimeNS, TimeUnit.NANOSECONDS, wl);
    }

    @Override
    protected void deleteSingleLock(KeyColumn lockID,
            ZooKeeperLockStatus lockStatus, StoreTransaction tx)
            throws Throwable {
        lockStatus.getWritelock().unlock();
        log.debug("Unlocked {} in ZK", lockID);
    }

    @Override
    protected void checkSingleLock(KeyColumn lockID,
            ZooKeeperLockStatus lockStatus, StoreTransaction tx)
            throws Throwable {
        // lockStatus.getWritelock().isOwner();
    }

}
