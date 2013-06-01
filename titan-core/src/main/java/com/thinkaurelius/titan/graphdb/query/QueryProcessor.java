package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.util.datastructures.Removable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

/**
 *
 * TODO: Make the returned iterator smarter about limits: If less than LIMIT elements are returned,
 * it checks if the underlying iterators have been exhausted. If not, then it doubles the limit, discards the first count
 * elements and returns the remaining ones. Tricky bit: how to keep track of which iterators have been exhausted?
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class QueryProcessor<Q extends Query<Q>,R> implements Iterable<R> {

    private static final Logger log = LoggerFactory.getLogger(QueryProcessor.class);


    private final Q query;
    private final List<Q> optimal;
    private final QueryExecutor<Q,R> executor;

    public QueryProcessor(Q query, QueryExecutor<Q, R> executor, QueryOptimizer<Q> optimizer) {
        Preconditions.checkNotNull(query);
        Preconditions.checkNotNull(executor);
        this.query = query;
        this.executor = executor;
        this.optimal = optimizer.optimize(query);
        log.debug("Optimized query [{}] into {} subqueries",query,optimal.size());
        for (Q q : optimal) Preconditions.checkArgument(!q.isInvalid());
    }

    @Override
    public Iterator<R> iterator() {
        if (optimal.isEmpty()) return Iterators.emptyIterator();
        else return new OuterIterator();
    }

    public Iterator<R> getUnwrappedIterator() {
        Iterator<R> iter = null;
        if (query.isSorted()) {

            /*
             * When optimal.size() == 1 && !executor.hasNew(query), it seems
             * that we assume that executor.execute(optimal.get(0)) returns
             * results already sorted.
             * 
             * TODO check this assumption in executor implementations and document it in the interface
             */
            for (int i=optimal.size()-1;i>=0;i--) {
                if (iter==null) iter = executor.execute(optimal.get(i));
                else iter = new MergeSortIterator<R>(executor.execute(optimal.get(i)),iter,query.getSortOrder(),query.hasUniqueResults());
            }

            if (executor.hasNew(query))  {
                final List<R> allNew= Lists.newArrayList(executor.getNew(query));
                Collections.sort(allNew,query.getSortOrder());
                iter = new MergeSortIterator<R>(allNew.iterator(),iter,query.getSortOrder(),query.hasUniqueResults());
            }
        } else {

            final Set<R> allNew;
            if (executor.hasNew(query)) {
                allNew = Sets.newHashSet(executor.getNew(query));
            } else {
                allNew = ImmutableSet.of();
            }
            if (optimal.size()==1) { //This case is just a premature optimization
                iter = executor.execute(optimal.get(0));
                if (!allNew.isEmpty()) {
                    iter = Iterators.filter(iter,new Predicate<R>() {
                        @Override
                        public boolean apply(@Nullable R r) {
                            return !allNew.contains(r);
                        }
                    });
                    iter = Iterators.concat(allNew.iterator(),iter);
                }
            } else {
                iter = Iterators.concat(Iterators.transform(optimal.iterator(),new Function<Q, Iterator<R>>() {
                    @Nullable
                    @Override
                    public Iterator<R> apply(@Nullable Q q) {
                        Iterator<R> iter = executor.execute(q);
                        if (!allNew.isEmpty()) {
                            iter = Iterators.filter(iter,new Predicate<R>() {
                                @Override
                                public boolean apply(@Nullable R r) {
                                    return !allNew.contains(r);
                                }
                            });
                        }
                        return iter;
                    }
                }));
                if (!allNew.isEmpty()) iter = Iterators.concat(allNew.iterator(),iter);

                if (query.hasUniqueResults()) {
                    final Set<R> seenResults = new HashSet<R>();
                    iter = Iterators.filter(iter,new Predicate<R>() {
                        @Override
                        public boolean apply(@Nullable R r) {
                            if (seenResults.contains(r)) return false;
                            else {
                                seenResults.add(r);
                                return true;
                            }
                        }
                    });
                }
            }
        }
        return iter;
    }

    private final class OuterIterator implements Iterator<R> {

        private final Iterator<R> iter;
        private final int limit;
        private final int skip;

        private R current;
        private R next;
        private int count;


        OuterIterator() {
            this.iter=getUnwrappedIterator();
            if (query.hasLimit()) limit = query.getLimit();
            else limit = Query.NO_LIMIT;
            this.skip = query.getSkip();
            count = 0;
            this.current=null;
            this.next = nextInternal();
            // Discard elements if requested
            for (int i = 0; i < this.skip && hasNext(); i++) {
                next();
            }
        }

        @Override
        public boolean hasNext() {
            return next!=null;
        }

        private R nextInternal() {
            R r = null;
            if (count<limit && iter.hasNext()) {
                r = iter.next();
            }
            return r;
        }

        @Override
        public R next() {
            if (!hasNext()) throw new NoSuchElementException();
            current = next;
            count++;
            next = nextInternal();
            return current;
        }

        @Override
        public void remove() {
            if (current instanceof Removable) ((Removable)current).remove();
            else throw new UnsupportedOperationException();
        }

    }


    /**
     * Iterate in sorted order over the combined elements of two existing sorted
     * iterators. If {@code filterDuplicates} is false, then "combined elements"
     * is like the sorted concatenation of both input iterators. If
     * {@code filterDuplicates} is true, then "combined elements" is like the
     * sorted list formed from the union of the elements in both input
     * iterators.
     * <p>
     * <b>Both input iterators must be sorted.</b>
     * 
     * @param <R>
     *            The iterator element type. Both input iterators must be of the
     *            same type and their elements should be mutually comparable.
     */
    private static final class MergeSortIterator<R> implements Iterator<R> {


        private final Iterator<R> first;
        private final Iterator<R> second;
        private final Comparator<R> comp;
        private final boolean filterDuplicates;

        private R nextFirst;
        private R nextSecond;
        private R next;

        public MergeSortIterator(Iterator<R> first, Iterator<R> second, Comparator<R> comparator, boolean filterDuplicates) {
            Preconditions.checkNotNull(first);
            Preconditions.checkNotNull(second);
            Preconditions.checkNotNull(comparator);
            this.first=first;
            this.second=second;
            this.comp=comparator;
            this.filterDuplicates=filterDuplicates;

            nextFirst =null;
            nextSecond =null;
            next = nextInternal();
        }

        @Override
        public boolean hasNext() {
            return next!=null;
        }

        /**
         * If the next elements on both input iterators are equal, then take and
         * return the element from {@code first}. Otherwise, take and return the
         * smaller element.
         */
        @Override
        public R next() {
            if (!hasNext()) throw new NoSuchElementException();
            R current = next;
            next = null;
            do {
                next = nextInternal();
            } while (next!=null && filterDuplicates && comp.compare(current,next)==0);
            return current;
        }

        public R nextInternal() {
            if (nextFirst==null && first.hasNext()) {
                nextFirst =first.next();
                assert nextFirst !=null;
            }
            if (nextSecond==null && second.hasNext()) {
                nextSecond =second.next();
                assert nextSecond !=null;
            }
            R result = null;
            if (nextFirst ==null && nextSecond ==null) {
                return null;
            } else if (nextFirst==null) {
                result=nextSecond;
                nextSecond=null;
            } else if (nextSecond==null) {
                result=nextFirst;
                nextFirst=null;
            } else {
                //Compare
                int c = comp.compare(nextFirst, nextSecond);
                if (c<=0) {
                    result= nextFirst;
                    nextFirst =null;
                } else {
                    result= nextSecond;
                    nextSecond =null;
                }
            }
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
