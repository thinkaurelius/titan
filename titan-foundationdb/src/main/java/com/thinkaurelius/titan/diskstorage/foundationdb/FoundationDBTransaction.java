package com.thinkaurelius.titan.diskstorage.foundationdb;

import com.foundationdb.FDBException;
import com.foundationdb.Transaction;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;

public class FoundationDBTransaction extends AbstractStoreTransaction {

    private Transaction tr;

    public FoundationDBTransaction(Transaction tr, StoreTxConfig config) {
        super(config);
        this.tr = tr;
    }

    public Transaction getTransaction() {
        return tr;
    }


    @Override
    public void commit() throws StorageException {
        if (tr == null) return;
        try {
            tr.commit().get();
        }
        catch (FDBException e) {
            if(e.getCode() == 1007) throw new TemporaryStorageException("Transaction was open for too long.", e.getCause());
            else throw new TemporaryStorageException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void rollback() throws StorageException {
        if (tr == null) return;
        tr.reset();
    }

    @Override
    public void flush() throws StorageException {
        if (tr == null) return;
        try {
            tr.commit().get();
        }
        catch (FDBException e) {
            if(e.getCode() == 1007) throw new TemporaryStorageException("Transaction was open for too long.", e.getCause());
            else throw new TemporaryStorageException(e.getMessage(), e.getCause());
        }
    }
}
