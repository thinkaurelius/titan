package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import java.util.Iterator;
import java.util.Map;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of storage backend properties in Accumulo.
 *
 * Each storage backend provides the functionality to get and set properties.
 * This class implements this backend properties using a single row and column
 * family in Accumulo.
 *
 * <code>AccumuloStoreConfiguration</code> is thread-safe.
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public class AccumuloStoreConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloStoreConfiguration.class);
    // Configuration defaults
    private static final String ROW_ID_DEFAULT = "";
    private static final String COL_FAMILY_DEFAULT = "_properties";
    private static final Authorizations AUTHORIZATIONS_DEFAULT = new Authorizations();
    // Instance variables
    private final Connector connector;  // thread-safe
    private final String tableName;
    private final AccumuloBatchConfiguration batchConfiguration;
    private final Text rowIdText;
    private final Text colFamilyText;

    /**
     * Construct Accumulo store configuration that gets and sets properties.
     *
     * @param connector Connection to Accumulo instance
     * @param tableName Accumulo table backing Titan store
     * @param batchConfiguration Configuration for batch operations
     */
    public AccumuloStoreConfiguration(Connector connector, String tableName, AccumuloBatchConfiguration batchConfiguration) {
        this(connector, tableName, batchConfiguration, ROW_ID_DEFAULT, COL_FAMILY_DEFAULT);
    }

    /**
     * Construct Accumulo store configuration that gets and sets properties.
     *
     * @param connector Connection to Accumulo instance
     * @param tableName Accumulo table backing Titan store
     * @param batchConfiguration Configuration for batch operations
     * @param rowId Row containing properties.
     * @param colFamily Column family for key-value pairs.
     */
    public AccumuloStoreConfiguration(Connector connector, String tableName,
            AccumuloBatchConfiguration batchConfiguration, String rowId, String colFamily) {
        this.connector = connector;
        this.tableName = tableName;
        this.batchConfiguration = batchConfiguration;
        this.rowIdText = new Text(rowId);
        this.colFamilyText = new Text(colFamily);
    }

    /**
     * Get Accumulo store property for {
     *
     * @ key}.
     *
     * @param key Property to get
     * @return Property value
     * @throws StorageException
     */
    public String getConfigurationProperty(String key) throws StorageException {
        Preconditions.checkArgument(key != null, "Key cannot be null");

        Scanner scanner;
        try {
            scanner = connector.createScanner(tableName, AUTHORIZATIONS_DEFAULT);
        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan table " + tableName, ex);
            throw new PermanentStorageException(ex);
        }

        Key scanKey = new Key(rowIdText, colFamilyText, new Text(key));
        scanner.setRange(new Range(scanKey, true, scanKey, true));

        Iterator<Map.Entry<Key, Value>> kvs = scanner.iterator();

        if (kvs.hasNext()) {
            return kvs.next().getValue().toString();
        } else {
            return null;
        }
    }

    /**
     * Sets Accumulo store property for {@ key} to {@ value}.
     *
     * @param key Property to set
     * @param value Property value
     * @throws StorageException
     */
    public void setConfigurationProperty(String key, String value) throws StorageException {
        try {
            BatchWriter writer = batchConfiguration.createBatchWriter(connector, tableName);
            try {
                Mutation mutation = new Mutation(rowIdText);
                mutation.put(colFamilyText, new Text(key), new Value(value.getBytes()));

                writer.addMutation(mutation);
                writer.flush();
            } catch (MutationsRejectedException ex) {
                logger.error("Can't set configuration on Titan store" + tableName, ex);
                throw new TemporaryStorageException(ex);
            } finally {
                try {
                    writer.close();
                } catch (MutationsRejectedException ex) {
                    logger.error("Can't set configuration on Titan store" + tableName, ex);
                    throw new TemporaryStorageException(ex);
                }
            }
        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }
    }
}
