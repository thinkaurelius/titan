package com.thinkaurelius.titan.diskstorage.accumulo.util;

/**
 * Callable function that throws exceptions for use with {@code ConcurrentLists}.
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public interface CallableFunction<F, T> {

    /**
     * Application of this function to {@code input}. 
     * 
     * @param input Function parameter
     * @return Computed value
     * @throws Exception 
     */
    public T apply(F input) throws Exception;
}
