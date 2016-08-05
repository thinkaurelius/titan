package com.thinkaurelius.titan.diskstorage.mapdb;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import org.mapdb.DB;
import org.mapdb.TxBlock;
import org.mapdb.TxMaker;
import org.mapdb.TxRollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapdBTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(MapdBTx.class);
    private DB tx = null;
    private TxMaker maker = null;

    public MapdBTx(BaseTransactionConfig config, MapDBKeyValueStore store) {
        super(config);
        maker = store.getTxMaker();
        tx = maker.makeTx();
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        tx.rollback();
        if (tx == null) return;
        if (log.isTraceEnabled())
            log.trace("{} rolled back", this.toString(), new TransactionClose(this.toString()));
            tx = null;

    }

    @Override
    public synchronized void commit() throws BackendException {
        super.commit();
        maker.execute(new TxBlock() {
            @Override
            public void tx(DB txB) throws TxRollbackException {
                txB = tx;
            }
        });
//        tx.commit();
        if (tx == null) return;
        if (log.isTraceEnabled())
            log.trace("{} committed", this.toString(), new TransactionClose(this.toString()));


    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + (null == tx ? "nulltx" : tx.toString());
    }

    public DB getTx() {
        return tx;
    }

    private static class TransactionClose extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionClose(String msg) {
            super(msg);
        }
    }
}
