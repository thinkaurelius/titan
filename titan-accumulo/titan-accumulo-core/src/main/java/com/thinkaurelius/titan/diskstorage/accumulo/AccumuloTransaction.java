package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;

/**
 * This creates a transaction type specific to Accumulo.
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public class AccumuloTransaction extends AbstractStoreTransaction {

    public AccumuloTransaction(ConsistencyLevel level) {
        super(ConsistencyLevel.KEY_CONSISTENT);
    }
}
