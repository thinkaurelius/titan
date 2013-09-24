package com.thinkaurelius.titan.diskstorage.accumulo.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Concurrent transformations on lists.
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public class ConcurrentLists {

    /**
     * Default size of executor thread pool.
     */
    public static final int NUM_THREADS_DEFAULT = 4;

    /**
     * Returns list that is concurrent application of {@code function} to each
     * element of {@code fromList}.
     *
     * @param <F> Element type of from list
     * @param <T> Element type of result list
     * @param fromList Source list
     * @param function Transformation function
     * @return List of function applications
     */
    public static <F, T> List<T> transform(List<F> fromList,
            final CallableFunction<? super F, ? extends T> function) {

        return transform(fromList, function, NUM_THREADS_DEFAULT);
    }

    /**
     * Returns list that is concurrent application of {@code function} to each
     * element of {@code fromList}.
     *
     * @param <F> Element type of from list
     * @param <T> Element type of result list
     * @param fromList Source list
     * @param function Transformation function
     * @param numThreads Size of thread pool
     * @return List of function applications
     */
    public static <F, T> List<T> transform(List<F> fromList,
            CallableFunction<? super F, ? extends T> function, int numThreads) {
        Preconditions.checkArgument(numThreads > 0, "numThreads must be > 0");

        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(numThreads);
            return transform(fromList, function, executor);
        } finally {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    /**
     * Returns list that is concurrent application of {@code function} to each
     * element of {@code fromList}.
     *
     * @param <F> Element type of from list
     * @param <T> Element type of result list
     * @param fromList Source list
     * @param function Transformation function
     * @param executor Executor service for threads
     * @return List of function applications
     */
    public static <F, T> List<T> transform(List<F> fromList,
            final CallableFunction<? super F, ? extends T> function, ExecutorService executor) {

        List<Callable<T>> tasks = Lists.newArrayListWithCapacity(fromList.size());
        for (final F from : fromList) {
            tasks.add(
                    new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return function.apply(from);
                }
            });
        }

        List<T> results = Lists.newArrayListWithCapacity(fromList.size());
        try {
            List<Future<T>> futures = executor.invokeAll(tasks);

            for (Future<T> future : futures) {
                results.add(future.get());
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException ex) {
            Throwable t = ex.getCause();
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else if (t instanceof Error) {
                throw (Error) t;
            } else {
                throw new IllegalStateException(t);
            }
        }

        return results;
    }
}