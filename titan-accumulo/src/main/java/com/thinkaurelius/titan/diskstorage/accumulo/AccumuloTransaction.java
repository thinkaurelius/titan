package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;

/**
 * This class overrides and adds nothing compared with
 * {@link com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockTransaction}; however, it creates a transaction type specific
 * to HBase, which lets us check for user errors like passing a Cassandra
 * transaction into a HBase method.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class AccumuloTransaction extends AbstractStoreTransaction {

    public AccumuloTransaction(ConsistencyLevel level) {
        super(ConsistencyLevel.KEY_CONSISTENT);
    }
}
