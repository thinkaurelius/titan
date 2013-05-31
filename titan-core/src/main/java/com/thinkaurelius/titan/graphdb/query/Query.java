package com.thinkaurelius.titan.graphdb.query;

import java.util.Comparator;

/**
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface Query<Q extends Query> {

    public static final int NO_LIMIT = Integer.MAX_VALUE;

    public boolean hasLimit();

    public int getLimit();
    
    /**
     * The number of initial results to discard.
     * 
     * @return A nonnegative count of skipped initial results
     */
    public int getSkip();

    public boolean isInvalid();

//    public Q updateLimit(int newLimit);

    public boolean isSorted();

    public Comparator getSortOrder();

    public boolean hasUniqueResults();

}
