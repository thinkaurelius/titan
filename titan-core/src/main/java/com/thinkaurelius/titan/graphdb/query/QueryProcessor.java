package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.TitanElement;
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

public class QueryProcessor<Q extends ElementQuery<R,B>,R extends TitanElement,B extends BackendQuery<B>> implements Iterable<R> {

    private static final Logger log = LoggerFactory.getLogger(QueryProcessor.class);


    private final Q query;
    private final QueryExecutor<Q,R,B> executor;

    public QueryProcessor(Q query, QueryExecutor<Q, R, B> executor) {
        Preconditions.checkNotNull(query);
        Preconditions.checkNotNull(executor);
        this.query = query;
        this.executor = executor;
    }

    @Override
    public Iterator<R> iterator() {
        if (query.isEmpty()) return Iterators.emptyIterator();
        else return new OuterIterator();
    }

    private final class OuterIterator implements Iterator<R> {

        private final Iterator<R> iter;
        private final int limit;

        private R current;
        private R next;
        private int count;


        OuterIterator() {
            this.iter=getUnwrappedIterator();
            if (query.hasLimit()) limit = query.getLimit();
            else limit = Query.NO_LIMIT;
            count = 0;
            this.current=null;
            this.next = nextInternal();
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

    private Iterator<R> getUnwrappedIterator() {
        Iterator<R> iter = null;
        boolean hasDeletions = executor.hasDeletions(query);
        Iterator<R> newElements = executor.getNew(query);
        if (query.isSorted()) {
            for (int i=query.numSubQueries()-1;i>=0;i--) {
                BackendQueryHolder<B> subq = query.getSubQuery(i);
                Iterator<R> subqiter = new LimitAdjustingIterator(subq);
                subqiter = getFilterIterator(subqiter,hasDeletions,!subq.isFitted());

                if (iter==null) iter = subqiter;
                else iter = new MergeSortIterator<R>(subqiter,iter,query.getSortOrder(),query.hasDuplicateResults());
            }
            Preconditions.checkArgument(iter!=null,"Query without sub-queries: %s",query);

            if (newElements.hasNext())  {
                final List<R> allNew= Lists.newArrayList(newElements);
                Collections.sort(allNew,query.getSortOrder());
                iter = new MergeSortIterator<R>(allNew.iterator(),iter,query.getSortOrder(),query.hasDuplicateResults());
            }
        } else {
            final Set<R> allNew;
            if (newElements.hasNext()) {
                allNew = Sets.newHashSet(newElements);
            } else {
                allNew = ImmutableSet.of();
            }

            List<Iterator<R>> iters = new ArrayList<Iterator<R>>(query.numSubQueries());
            for (int i=0;i<query.numSubQueries();i++) {
                BackendQueryHolder<B> subq = query.getSubQuery(i);
                Iterator subiter = new LimitAdjustingIterator(subq);
                subiter = getFilterIterator(subiter,hasDeletions,!subq.isFitted());
                if (!allNew.isEmpty()) {
                    subiter = Iterators.filter(subiter,new Predicate<R>() {
                        @Override
                        public boolean apply(@Nullable R r) {
                            return !allNew.contains(r);
                        }
                    });
                }
                iters.add(subiter);
            }
            if (iters.size()>1) {
                iter = Iterators.concat(iters.iterator());
                if (query.hasDuplicateResults()) { //Cache results and filter out duplicates
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
            } else iter = iters.get(0);

            if (!allNew.isEmpty()) iter = Iterators.concat(allNew.iterator(),iter);
        }
        return iter;
    }

    private final Iterator<R> getFilterIterator(final Iterator<R> iter, final boolean filterDeletions, final boolean filterMatches) {
        if (filterDeletions || filterMatches) {
            return Iterators.filter(iter,new Predicate<R>(){
                @Override
                public boolean apply(@Nullable R r) {
                    return (!filterDeletions || !executor.isDeleted(query,r)) && (!filterMatches || query.matches(r));
                }
            });
        } else {
            return iter;
        }
    }

    private final class LimitAdjustingIterator implements Iterator<R> {

        private B backendQuery;
        private final Object executionInfo;
        private int currentLimit;
        private int count;

        private Iterator<R> iter;


        private LimitAdjustingIterator(BackendQueryHolder<B> backendQueryHolder) {
            this.backendQuery = backendQueryHolder.getBackendQuery();
            this.executionInfo = backendQueryHolder.getExecutionInfo();
            this.currentLimit = backendQuery.getLimit();
            this.count = 0;
            this.iter = executor.execute(query,backendQuery,executionInfo);
        }

        @Override
        public boolean hasNext() {
            if (count<currentLimit) return iter.hasNext();
            else {
                //Update query and iterate through
                currentLimit = (int)Math.min(Integer.MAX_VALUE-1,Math.round(currentLimit*2.0));
                backendQuery = backendQuery.updateLimit(currentLimit);
                iter = executor.execute(query,backendQuery,executionInfo);
                for (int i=0;i<count;i++) iter.next();
                Preconditions.checkArgument(count<currentLimit);
                return hasNext();
            }
        }

        @Override
        public R next() {
            if (!hasNext()) throw new NoSuchElementException();
            count++;
            return iter.next();
        }

        @Override
        public void remove() {
            iter.remove();
        }
    }


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
