package com.thinkaurelius.titan.diskstorage.mapdb;

import com.sleepycatje.je.DatabaseEntry;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TxMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MapDBKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(MapDBKeyValueStore.class);

    private static final StaticBuffer.Factory<DBEntry> ENTRY_FACTORY = new StaticBuffer.Factory<DBEntry>() {
        @Override
        public DBEntry get(byte[] array, int offset, int limit) {
            return new DBEntry(array,offset,limit-offset);
        }
    };

    private final TxMaker txMaker;
    private final String name;
    private final MapDBStoreManager manager;
    private boolean isOpen;

    /**
     * Creates a file backed MapDB where the file path will be dir / storeName.mapdb
     *
     * @param n   name of this store
     * @param m   Store manager handles multiple store management
     * @param dir Directory in which the backing file will be created
     */
    public MapDBKeyValueStore(String n, MapDBStoreManager m, File dir) {
        name = n;
        manager = m;
        txMaker = DBMaker.newFileDB(new File(dir, n + ".mapdb")).makeTxMaker();
        isOpen = true;
    }

    @Override
    public String getName() {
        return name;
    }


    @Override
    public synchronized void close() throws BackendException {
        if (isOpen) txMaker.close();
        if (isOpen) manager.removeDatabase(this);
        isOpen = false;
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        boolean noTX = (txh == null);
        DB tx = (noTX ? txMaker.makeTx() : ((MapdBTx) txh).getTx());
        Map<DBEntry, DBEntry> map = tx.getTreeMap(name);
        StaticBuffer res =  new StaticArrayBuffer(getBuffer(map.get(key.as(ENTRY_FACTORY))));
        if (noTX)
            tx.commit();
        return res;
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key,txh)!=null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
//        if (getTransaction(txh) == null) {
//            log.warn("Attempt to acquire lock with transactions disabled");
//        } //else we need no locking
        //noop
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        final StaticBuffer keyStart = query.getStart();
        final StaticBuffer keyEnd = query.getEnd();
        final boolean noTX = (txh == null);
        final MapdBTx mTxH = (noTX ? null : (MapdBTx) txh);
        return new RecordIterator<KeyValueEntry>() {
            DB tx = (noTX ? txMaker.makeTx() : mTxH.getTx());
            BTreeMap<DBEntry, DBEntry> map = tx.getTreeMap(name);

            private final Iterator<Map.Entry<DBEntry,DBEntry>> entries = map.subMap(keyStart.as(ENTRY_FACTORY), keyEnd.as(ENTRY_FACTORY)).entrySet().iterator();

                @Override
                public boolean hasNext() {
                    return entries.hasNext();
                }

                @Override
                public KeyValueEntry next() {
                    Map.Entry<DBEntry,DBEntry> ent = entries.next();
                    return new KeyValueEntry(getBuffer(ent.getKey()),getBuffer(ent.getValue()));
                }

                @Override
                public void close() {
//                    tx.commit();
//                    tx.close();
                }

            @Override
            public void remove()  {
                    throw new UnsupportedOperationException();
                }
        };

    }

    protected TxMaker getTxMaker() {
        return txMaker;
    }

    @Override
    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws BackendException {
        boolean noTX = (txh == null);
        DB tx = (noTX ? txMaker.makeTx() : ((MapdBTx) txh).getTx());
        tx.getTreeMap(name).put(key.as(ENTRY_FACTORY), value.as(ENTRY_FACTORY));
        if (noTX) {
            tx.commit();
            tx.close();}
    }


    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) {
        boolean noTX = (txh == null);
        DB tx = (noTX ? txMaker.makeTx() : ((MapdBTx) txh).getTx());
        tx.getTreeMap(name).remove(key.as(ENTRY_FACTORY));
        if (noTX) {
            tx.commit();
            tx.close();
        }

    }

    public void clear() {
        DB tx = txMaker.makeTx();
        tx.getTreeMap(name).clear();
        tx.commit();
        tx.close();
    }

    private static StaticBuffer getBuffer(DBEntry entry) {
        return new StaticArrayBuffer(entry.getData(),entry.getOffset(),entry.getOffset()+entry.getSize());
    }
}
