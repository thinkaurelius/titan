package org.titan.cassandra.cql;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

public class CqlTransaction extends AbstractStoreTransaction {

	private final Consistency readConsistency;
	private final Consistency writeConsistency;

	public CqlTransaction(ConsistencyLevel level, Consistency readConsistency,
			Consistency writeConsistency) {
		super(level);
		if (level == ConsistencyLevel.KEY_CONSISTENT) {
			this.readConsistency = Consistency.QUORUM;
			this.writeConsistency = Consistency.QUORUM;
		} else {
			Preconditions.checkNotNull(readConsistency);
			Preconditions.checkNotNull(writeConsistency);
			this.readConsistency = readConsistency;
			this.writeConsistency = writeConsistency;
		}
	}

	public Consistency getWriteConsistency() {
		return writeConsistency;
	}

	public Consistency getReadConsistency() {
		return readConsistency;
	}

	public static CqlTransaction getTx(StoreTransaction txh) {
		Preconditions.checkArgument(txh != null
				&& (txh instanceof CqlTransaction));
		return (CqlTransaction) txh;
	}

}
