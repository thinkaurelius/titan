package com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class NoKCVSCache extends KCVSCache {


    public NoKCVSCache(KeyColumnValueStore store) {
        super(store, null);
    }

    @Override
    public void clearCache() {
    }

    @Override
    protected void invalidate(StaticBuffer key, List<CachableStaticBuffer> entries) {
    }

}
