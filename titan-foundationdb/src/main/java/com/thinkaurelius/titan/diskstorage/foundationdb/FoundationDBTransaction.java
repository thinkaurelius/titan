package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.foundationdb.FDBException;
import com.foundationdb.Transaction;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;
import com.thinkaurelius.titan.diskstorage.TemporaryBackendException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBTransaction extends AbstractStoreTransaction {
    private static final Logger logger = LoggerFactory.getLogger(FoundationDBTransaction.class);

    private final Transaction tr;

    public FoundationDBTransaction(Transaction tr, BaseTransactionConfig config) {
        super(config);
        this.tr = tr;
        if (logger.isTraceEnabled()) {
            logger.trace("Begin {}", this, new Throwable());
        }
    }

    public Transaction getTransaction() {
        return tr;
    }


    @Override
    public void commit() throws BackendException {
        if (logger.isTraceEnabled()) {
            logger.trace("Commit {}", this, new Throwable());
        }
        try {
            tr.commit().get();
        }
        catch (FDBException e) {
            throw wrapException(e);
        }
    }

    public static BackendException wrapException(FDBException e) {
        switch (e.getCode()) {
        case 1007:              // past_version
            return new TemporaryBackendException("Transaction was open for too long.", e);
        case 1000:              // operation_failed
        case 1004:              // timed_out
        case 1009:              // future_version
        case 1020:              // not_committed
        case 1021:              // commit_unknown_result
            return new TemporaryBackendException(e.getMessage(), e);
        default:
            return new PermanentBackendException(e.getMessage(), e);
        }
    }

    @Override
    public void rollback() throws BackendException {
        if (logger.isTraceEnabled()) {
            logger.trace("Rollback {}", this, new Throwable());
        }
        tr.reset();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + tr + ")";
    }

}
