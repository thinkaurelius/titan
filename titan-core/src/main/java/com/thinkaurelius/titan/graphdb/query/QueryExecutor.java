package com.thinkaurelius.titan.graphdb.query;

import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface QueryExecutor<Q extends Query<Q>,R> {

    /**
     * Check whether this executor's transaction might have in-memory elements
     * (not yet written to a storage backend) that match {@code query}. This is
     * allowed to return false positives.
     * 
     * @param query
     *            The query to check
     * @return True if this executor's transaction might have created
     *         uncommitted in-memory elements that match {@code query}, false
     *         otherwise
     */
    public boolean hasNew(Q query);

    /**
     * Get new in-memory element matches for {@code query} after
     * {@link #hasNew(query)} returns true. Probably returns an empty iterator
     * when {@code hasNew(query)} is false, though precise behavior under that
     * circumstance is undefined.
     * 
     * @param query
     *            The query to match
     * @return Newly created in-memory elements matching {@code query}
     */
    public Iterator<R> getNew(Q query);

    /**
     * Retrieve query results from backend storage. The returned iterator must
     * exclude elements that no longer match {@code query} due to uncommitted
     * modifications held in memory by this executor's transaction. However,
     * this method need not return elements newly created by this executor's
     * transaction which haven't yet been committed to a storage backend. To
     * find in-memory elements that match {@code query}, see
     * {@link #hasNew(Query)} and {@link #getNew(Query)}.
     * 
     * @param query
     *            The query to run against storage
     * @return Results from the storage backend
     */
    public Iterator<R> execute(Q query);

}
