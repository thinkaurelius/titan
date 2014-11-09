package com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.ExpirationKCVSCache.OffHeapQuery;

import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntryList;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import junit.framework.Assert;
import org.junit.Test;

public class ExpiringKCVSCacheTest {
    @Test
    public void testOffHeapQuery() throws Exception {
        OffHeapQuery query = new OffHeapQuery(getQuery(2, 8), StaticArrayEntryList.of(getEntry(3, 5), getEntry(7, 9)));
        Assert.assertTrue(query.compareTo(getQuery(2, 8)) == 0);
        Assert.assertTrue(query.compareTo(getQuery(0, 10)) != 0);

        Assert.assertTrue(query.equals(getQuery(2, 8)));
        Assert.assertFalse(query.equals(getQuery(0, 10)));
    }

    public static SliceQuery getQuery(int startCol, int endCol) {
        return new SliceQuery(BufferUtil.getIntBuffer(startCol),BufferUtil.getIntBuffer(endCol));
    }

    public static Entry getEntry(int col, int val) {
        return new StaticArrayEntry(new WriteByteBuffer(4 * 2).putInt(col).putInt(val).getStaticBuffer(), 4);
    }
}
