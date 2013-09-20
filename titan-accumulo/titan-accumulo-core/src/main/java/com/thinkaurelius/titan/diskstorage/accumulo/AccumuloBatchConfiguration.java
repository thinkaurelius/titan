package com.thinkaurelius.titan.diskstorage.accumulo;

import java.util.concurrent.TimeUnit;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Configure Accumulo 1.4.3 batch scanners, writers and deleters.
 *
 * Adapted from org.apache.accumulo.core.client.BatchWriterConfig.
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public class AccumuloBatchConfiguration {

    private static final Integer DEFAULT_NUM_QUERY_THREADS = 3;
    private Integer numQueryThreads = null;
    private static final Long DEFAULT_MAX_MEMORY = 50 * 1024 * 1024l;
    private Long maxMemory = null;
    private static final Long DEFAULT_MAX_LATENCY = 2 * 60 * 1000l;
    private Long maxLatency = null;
    private static final Long DEFAULT_TIMEOUT = Long.MAX_VALUE;
    private Long timeout = null;
    private static final Integer DEFAULT_MAX_WRITE_THREADS = 3;
    private Integer maxWriteThreads = null;

    /**
     * Sets the number of threads to spawn for querying tablet servers.
     *
     * <p>
     * <b>Default:</b> 3
     *
     * @param numQueryThreads the number threads to use
     * @throws IllegalArgumentException if {@code maxWriteThreads} is
     * non-positive
     * @return {@code this} to allow chaining of set methods
     */
    public AccumuloBatchConfiguration setNumQueryThreads(int numQueryThreads) {
        if (numQueryThreads <= 0) {
            throw new IllegalArgumentException("Num threads must be positive " + numQueryThreads);
        }

        this.numQueryThreads = numQueryThreads;
        return this;
    }

    /**
     * Sets the maximum memory to batch before writing. The smaller this value,
     * the more frequently the {@link BatchWriter} will write.<br />
     * If set to a value smaller than a single mutation, then it will
     * {@link BatchWriter#flush()} after each added mutation. Must be
     * non-negative.
     *
     * <p>
     * <b>Default:</b> 50M
     *
     * @param maxMemory max size in bytes
     * @throws IllegalArgumentException if {@code maxMemory} is less than 0
     * @return {@code this} to allow chaining of set methods
     */
    public AccumuloBatchConfiguration setMaxMemory(long maxMemory) {
        if (maxMemory < 0) {
            throw new IllegalArgumentException("Max memory must be non-negative.");
        }
        this.maxMemory = maxMemory;
        return this;
    }

    /**
     * Sets the maximum amount of time to hold the data in memory before
     * flushing it to servers.<br />
     * For no maximum, set to zero, or {@link Long#MAX_VALUE} with
     * {@link TimeUnit#MILLISECONDS}.
     *
     * <p> {@link TimeUnit#MICROSECONDS} or {@link TimeUnit#NANOSECONDS} will be
     * truncated to the nearest {@link TimeUnit#MILLISECONDS}.<br />
     * If this truncation would result in making the value zero when it was
     * specified as non-zero, then a minimum value of one
     * {@link TimeUnit#MILLISECONDS} will be used.
     *
     * <p>
     * <b>Default:</b> 120 seconds
     *
     * @param maxLatency the maximum latency, in the unit specified by the value
     * of {@code timeUnit}
     * @param timeUnit determines how {@code maxLatency} will be interpreted
     * @throws IllegalArgumentException if {@code maxLatency} is less than 0
     * @return {@code this} to allow chaining of set methods
     */
    public AccumuloBatchConfiguration setMaxLatency(long maxLatency, TimeUnit timeUnit) {
        if (maxLatency < 0) {
            throw new IllegalArgumentException("Negative max latency not allowed " + maxLatency);
        }

        if (maxLatency == 0) {
            this.maxLatency = Long.MAX_VALUE;
        } else { // make small, positive values that truncate to 0 when converted use the minimum millis instead
            this.maxLatency = Math.max(1, timeUnit.toMillis(maxLatency));
        }
        return this;
    }

    /**
     * Sets the maximum amount of time an unresponsive server will be re-tried.
     * When this timeout is exceeded, the {@link BatchWriter} should throw an
     * exception.<br />
     * For no timeout, set to zero, or {@link Long#MAX_VALUE} with
     * {@link TimeUnit#MILLISECONDS}.
     *
     * <p> {@link TimeUnit#MICROSECONDS} or {@link TimeUnit#NANOSECONDS} will be
     * truncated to the nearest {@link TimeUnit#MILLISECONDS}.<br />
     * If this truncation would result in making the value zero when it was
     * specified as non-zero, then a minimum value of one
     * {@link TimeUnit#MILLISECONDS} will be used.
     *
     * <p>
     * <b>Default:</b> {@link Long#MAX_VALUE} (no timeout)
     *
     * @param timeout the timeout, in the unit specified by the value of
     * {@code timeUnit}
     * @param timeUnit determines how {@code timeout} will be interpreted
     * @throws IllegalArgumentException if {@code timeout} is less than 0
     * @return {@code this} to allow chaining of set methods
     */
    public AccumuloBatchConfiguration setTimeout(long timeout, TimeUnit timeUnit) {
        if (timeout < 0) {
            throw new IllegalArgumentException("Negative timeout not allowed " + timeout);
        }

        if (timeout == 0) {
            this.timeout = Long.MAX_VALUE;
        } else { // make small, positive values that truncate to 0 when converted use the minimum millis instead
            this.timeout = Math.max(1, timeUnit.toMillis(timeout));
        }
        return this;
    }

    /**
     * Sets the maximum number of threads to use for writing data to the tablet
     * servers.
     *
     * <p>
     * <b>Default:</b> 3
     *
     * @param maxWriteThreads the maximum threads to use
     * @throws IllegalArgumentException if {@code maxWriteThreads} is
     * non-positive
     * @return {@code this} to allow chaining of set methods
     */
    public AccumuloBatchConfiguration setMaxWriteThreads(int maxWriteThreads) {
        if (maxWriteThreads <= 0) {
            throw new IllegalArgumentException("Max threads must be positive " + maxWriteThreads);
        }

        this.maxWriteThreads = maxWriteThreads;
        return this;
    }

    /**
     *  Get number of threads to spawn for querying on tablet servers.
     * 
     * @return number of threads
     */
    public int getNumQueryThreads() {
        return numQueryThreads != null ? numQueryThreads : DEFAULT_NUM_QUERY_THREADS;
    }

    /**
     * Sets the maximum memory to batch before writing.
     * 
     * @return max memory
     */
    public long getMaxMemory() {
        return maxMemory != null ? maxMemory : DEFAULT_MAX_MEMORY;
    }

    /**
     * Sets the maximum amount of time to hold the data in memory before flushing it to servers.
     * 
     * @param timeUnit units for return value
     * @return max latency
     */
    public long getMaxLatency(TimeUnit timeUnit) {
        return timeUnit.convert(maxLatency != null ? maxLatency : DEFAULT_MAX_LATENCY, TimeUnit.MILLISECONDS);
    }

    /**
     * Get the maximum amount of time an unresponsive server will be re-tried.
     * 
     * @param timeUnit units for return value
     * @return max timeout
     */
    public long getTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(timeout != null ? timeout : DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * Get the maximum number of threads to use for writing data to the tablet servers.
     * 
     * @return max threads 
     */
    public int getMaxWriteThreads() {
        return maxWriteThreads != null ? maxWriteThreads : DEFAULT_MAX_WRITE_THREADS;
    }

    /**
     * Factory method to create a BatchDeleter connected to Accumulo.
     * 
     * @param connector connection to Accumulo
     * @param tableName the name of the table to query and delete from
     * @param authorizations set of authorization labels that will be checked against the column visibility.
     * @return BatchDeleter object for configuring and deleting
     * @throws TableNotFoundException when the specified table doesn't exist
     */
    public BatchDeleter createBatchDeleter(Connector connector, String tableName, Authorizations authorizations) throws TableNotFoundException {
        return connector.createBatchDeleter(tableName, authorizations,
                getNumQueryThreads(), getMaxMemory(), getMaxLatency(TimeUnit.MILLISECONDS), getMaxWriteThreads());
    }

    /**
     * Factory method to create a BatchScanner connected to Accumulo.
     * 
     * @param connector connection to Accumulo
     * @param tableName the name of the table to query and delete from
     * @param authorizations set of authorization labels that will be checked against the column visibility.
     * @return BatchScanner object for configuring and querying
     * @throws TableNotFoundException when the specified table doesn't exist
     */
    public BatchScanner createBatchScanner(Connector connector, String tableName, Authorizations authorizations) throws TableNotFoundException {
        return connector.createBatchScanner(tableName, authorizations, getNumQueryThreads());
    }

    /**
     * Factory method to create a BatchWriter connected to Accumulo.
     * 
     * @param connector connection to Accumulo
     * @param tableName the name of the table to insert data into
     * @return BatchWriter object for configuring and writing data
     * @throws TableNotFoundException when the specified table doesn't exist
     */
    public BatchWriter createBatchWriter(Connector connector, String tableName) throws TableNotFoundException {
        return connector.createBatchWriter(tableName,
                getMaxMemory(), getMaxLatency(TimeUnit.MILLISECONDS), getMaxWriteThreads());
    }
}