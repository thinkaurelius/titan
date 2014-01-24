package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnCounterStore;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import org.junit.Assert;
import org.junit.Test;

public abstract class CounterStoreTest {
    private final static StaticBuffer KEY = new StaticArrayBuffer("CounterStoreTest".getBytes());


    private final KeyColumnCounterStore store;

    public CounterStoreTest(KeyColumnCounterStore store) {
        this.store = store;
    }

    @Test
    public void testSingleColumnIncrement() throws Exception {
        testIncrements(1);
    }

    @Test
    public void testIncrementOfMultipleColumns() throws Exception {
        testIncrements(3);
    }

    @Test
    public void testCounterOfNoExistingColumn() throws Exception {
        Assert.assertEquals(0L, store.get(KEY, new StaticArrayBuffer("DefinitelyNotExists".getBytes())));
    }

    private void testIncrements(int numOfColumns) throws Exception {
        for (int i = 0; i < numOfColumns; i++) {
            StaticBuffer column = new StaticArrayBuffer((store.hashCode() + "_simple_counter").getBytes());

            store.clear(KEY, column);
            Assert.assertEquals(0L, store.get(KEY, column));

            // let's first increment by positive number
            store.increment(KEY, column, 2);
            Assert.assertEquals(0L, store.get(KEY, column));

            // then let's do decrement to see if we can fall to negative numbers
            store.increment(KEY, column, -3);
            Assert.assertEquals(-1L, store.get(KEY, column));

            // increment again to see if it works correctly with negatives
            store.increment(KEY, column, 3);
            Assert.assertEquals(2L, store.get(KEY, column));
        }
    }
}
