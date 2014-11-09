package com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache;

import com.google.common.base.Preconditions;
import com.google.common.cache.*;

import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;

import com.thinkaurelius.titan.diskstorage.util.VInt;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.thinkaurelius.titan.util.datastructures.ByteSize.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ExpirationKCVSCache extends KCVSCache {
    private static final Logger logger = LoggerFactory.getLogger(ExpirationKCVSCache.class);

    private static final ByteBufAllocator ALLOCATOR = new PooledByteBufAllocator();

    //Weight estimation
    private static final int STATICARRAYBUFFER_SIZE = STATICARRAYBUFFER_RAW_SIZE + 10; // 10 = last number is average length
    private static final int KEY_QUERY_SIZE = OBJECT_HEADER + 4 + 1 + 3 * (OBJECT_REFERENCE + STATICARRAYBUFFER_SIZE); // object_size + int + boolean + 3 static buffers

    private final LoadingCache<StaticBuffer, QueryContainer> cache;

    public ExpirationKCVSCache(final KeyColumnValueStore store, String metricsName,
                               long cacheTimeMS, long invalidationGracePeriodMS, long maximumByteSize) {
        super(store, metricsName);

        Preconditions.checkArgument(cacheTimeMS > 0, "Cache expiration must be positive: %s", cacheTimeMS);
        Preconditions.checkArgument(cacheTimeMS < TimeUnit.DAYS.toMillis(10 * 365), "Cache expiration time too large, overflow may occur: %s",cacheTimeMS);
        Preconditions.checkArgument(invalidationGracePeriodMS >= 0,"Invalid expiration grace period: %s", invalidationGracePeriodMS);

        cache = CacheBuilder.newBuilder()
                .maximumWeight(maximumByteSize)
                .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                .initialCapacity(1000)
                .expireAfterWrite(cacheTimeMS, TimeUnit.MILLISECONDS)
                .weigher(new Weigher<StaticBuffer, QueryContainer>() {
                    @Override
                    public int weigh(StaticBuffer key, QueryContainer entries) {
                        return GUAVA_CACHE_ENTRY_SIZE + KEY_QUERY_SIZE + entries.getByteSize();
                    }
                }).build(new CacheLoader<StaticBuffer, QueryContainer>() {
                    @Override
                    public QueryContainer load(StaticBuffer key) throws Exception {
                        return new QueryContainer();
                    }
                });
    }

    @Override
    public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh) throws BackendException {
        return cache.getUnchecked(query.getKey()).get(query, txh);
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, EntryList> results = new HashMap<StaticBuffer, EntryList>();
        for (StaticBuffer key : keys) {
            results.put(key, getSlice(new KeySliceQuery(key, query), txh));
        }

        return results;
    }

    @Override
    protected void invalidate(StaticBuffer key, List<CachableStaticBuffer> entries) {
        cache.invalidate(key);
    }

    @Override
    public void clearCache() {
        cache.invalidateAll();
    }

    private class QueryContainer {
        private final CopyOnWriteArraySet<OffHeapQuery> queries;

        public QueryContainer() {
            queries = new CopyOnWriteArraySet<OffHeapQuery>();
        }

        public EntryList get(KeySliceQuery query, StoreTransaction tx) throws BackendException {
            for (OffHeapQuery q : queries) {
                if (q.compareTo(query) == 0)
                    return q.results;
            }

            EntryList results = store.getSlice(query, unwrapTx(tx));
            queries.add(new OffHeapQuery(query, results));
            return results;
        }

        public int getByteSize() {
            int size = OBJECT_HEADER;
            for (OffHeapQuery q : queries)
                size += OBJECT_REFERENCE + q.getByteSize();

            return size;
        }
    }

    protected static class OffHeapQuery implements Comparable<SliceQuery> {
        private final ByteBuf query;
        private final EntryList results;

        public OffHeapQuery(SliceQuery query, EntryList results) {
            this.query = query.serialize(ALLOCATOR);
            this.results = results;
        }

        @Override
        public int compareTo(SliceQuery o) {
            if (o == null)
                return 1;

            int cmp;
            ByteBuf dup = query.duplicate();

            byte[] sliceStart = new byte[(int) VInt.decode(dup)];
            dup.readBytes(sliceStart);
            if ((cmp = o.getSliceStart().compareTo(new StaticArrayBuffer(sliceStart))) != 0)
                return cmp;

            byte[] sliceEnd = new byte[(int) VInt.decode(dup)];
            dup.readBytes(sliceEnd);
            if ((cmp = o.getSliceEnd().compareTo(new StaticArrayBuffer(sliceEnd))) != 0)
                return cmp;

            return Integer.compare((int) VInt.decode(dup), o.getLimit());
        }

        @Override
        public boolean equals(Object o) {
            return !(o == null || !(o instanceof SliceQuery)) && compareTo((SliceQuery) o) == 0;
        }

        public int getByteSize() {
            return OBJECT_HEADER + (2 * OBJECT_REFERENCE) + query.readableBytes() + results.getByteSize();
        }
    }
}
