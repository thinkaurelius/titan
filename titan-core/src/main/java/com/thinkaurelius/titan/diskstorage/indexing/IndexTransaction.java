package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wraps the transaction handle of an index and buffers all mutations against an index for efficiency.
 * Also acts as a proxy to the {@link IndexProvider} methods.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexTransaction implements TransactionHandle {

    private static final int DEFAULT_OUTER_MAP_SIZE = 3;
    private static final int DEFAULT_INNER_MAP_SIZE = 5;

    private final IndexProvider index;
    private final TransactionHandle indexTx;
    private Map<String,Map<String,IndexMutation>> mutations;

    public IndexTransaction(final IndexProvider index) throws StorageException {
        Preconditions.checkNotNull(index);
        this.index=index;
        this.indexTx=index.beginTransaction();
        Preconditions.checkNotNull(indexTx);
        this.mutations = null;
    }

    public void add(String storeName, String docId, String key, Object value, boolean isNew) {
        getIndexMutation(storeName,docId,isNew,false).addition(new IndexEntry(key,value));
    }

    public void delete(String storeName, String docId, String key, boolean deleteAll) {
        getIndexMutation(storeName,docId,false,deleteAll).deletion(key);
    }

    private IndexMutation getIndexMutation(String storeName, String docId, boolean isNew, boolean isDeleted) {
        if (mutations==null) mutations = new HashMap<String,Map<String,IndexMutation>>(DEFAULT_OUTER_MAP_SIZE);
        Map<String,IndexMutation> storeMutations = mutations.get(storeName);
        if (storeMutations==null) {
            storeMutations = new HashMap<String,IndexMutation>(DEFAULT_INNER_MAP_SIZE);
            mutations.put(storeName,storeMutations);

        }
        IndexMutation m = storeMutations.get(docId);
        if (m==null) {
            m = new IndexMutation(isNew,isDeleted);
            storeMutations.put(docId, m);
        }
        return m;
    }


    public void register(String store, String key, Class<?> dataType) throws StorageException {
        index.register(store,key,dataType,indexTx);
    }

    public List<String> query(IndexQuery query) throws StorageException {
        return index.query(query,indexTx);
    }

    @Override
    public void commit() throws StorageException {
        flushInternal();
        indexTx.commit();
    }

    @Override
    public void rollback() throws StorageException {
        mutations=null;
        indexTx.rollback();
    }

    @Override
    public void flush() throws StorageException {
        flushInternal();
        indexTx.flush();
    }

    private void flushInternal() throws StorageException {
        if (mutations!=null && !mutations.isEmpty()) {
            index.mutate(mutations,indexTx);
            mutations=null;
        }
    }

}
