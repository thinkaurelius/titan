/**
 *
 * This package borrows heavily from the lock recipe shipped with ZooKeeper 3.4.5.
 * The following files were originally copied verbatim from the directory
 * <code>src/recipes/lock/src/java/org/apache/zookeeper/recipes/lock</code>
 * in the ZooKeeper 3.4.5 source distribution into this package.
 * A few were modified after copying to support automatic lock expiration.
 *
 * <ul>
 * <li>LockListener.java</li>
 * <li>ProtocolSupport.java</li>
 * <li>WriteLock.java</li>
 * <li>ZNodeName.java</li>
 * <li>ZooKeeperOperation.java</li>
 * </ul>
 */
 package com.thinkaurelius.titan.diskstorage.locking.zookeeper;