package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.junit.Test;

import java.util.concurrent.ExecutionException;


public class BerkeleyVariableLengthKCVSTest extends KeyColumnValueStoreTest {

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        BerkeleyJEStoreManager sm = new BerkeleyJEStoreManager(BerkeleyStorageSetup.getBerkeleyJEConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

    @Override
    @Test
    public void testGetKeysWithKeyRange() throws Exception {
        super.testGetKeysWithKeyRange();
    }

    @Test @Override
    public void testConcurrentGetSlice() throws ExecutionException, InterruptedException, BackendException {

    }

    @Test @Override
    public void testConcurrentGetSliceAndMutate() throws BackendException, ExecutionException, InterruptedException {

    }
}
