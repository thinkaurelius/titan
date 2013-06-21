package com.thinkaurelius.titan.diskstorage.accumulo.util;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import java.io.File;
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
 * Implementation of storage backend properties using a local configuration
 * file.
 *
 * Each storage backend provides the functionality to get and set properties for
 * that particular backend. This class implementation this feature using a local
 * configuration file. Hence, it is only suitable for local storage backends.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class AccumuloStorageConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloStorageConfiguration.class);
    private static final String COL_FAMILY_DEFAULT = "_properties";
    private static final String ROW_ID_DEFAULT = "";
    private static final Long MAX_MEMORY_DEFAULT = 50 * 1024 * 1024l;
    private static final Long MAX_LATENCY_DEFAULT = 100l;
    private static final Integer MAX_WRITE_THREADS_DEFAULT = 10;
    private final Connector connector;
    private final String tableName;
    private final String colFamily;

    public AccumuloStorageConfiguration(Connector connector, String tableName) {
        this(connector, tableName, COL_FAMILY_DEFAULT);
    }

    public AccumuloStorageConfiguration(Connector connector, String tableName, String colFamily) {
        Preconditions.checkNotNull(connector);
        Preconditions.checkNotNull(tableName);
        Preconditions.checkArgument(tableName.length() > 0, "Table name is empty");
        this.connector = connector;
        this.tableName = tableName;
        this.colFamily = colFamily;
    }

    public String getConfigurationProperty(String key) throws StorageException {
        try {
            Scanner scanner = connector.createScanner(tableName, new Authorizations());
            
            Key scanKey = new Key(new Text(ROW_ID_DEFAULT), new Text(colFamily), new Text(key));
            scanner.setRange(new Range(key));
            
            if (scanner.iterator().hasNext()) {
                return scanner.iterator().next().getValue().toString();
            } else {
                return null;
            }
        } catch (TableNotFoundException ex) {
            logger.error("Can't find storage table " + tableName, ex);
            throw new PermanentStorageException(ex);
        }
    }

    public void setConfigurationProperty(String key, String value) throws StorageException {
        try {
            BatchWriter writer = connector.createBatchWriter(tableName,
                    MAX_MEMORY_DEFAULT, MAX_LATENCY_DEFAULT, MAX_WRITE_THREADS_DEFAULT);

            Mutation mutation = new Mutation(ROW_ID_DEFAULT);
            mutation.put(COL_FAMILY_DEFAULT, key, new Value(value.getBytes()));
            
            writer.addMutation(mutation);
            
            writer.close();
        } catch (TableNotFoundException ex) {
            logger.error("Can't find storage table " + tableName, ex);
            throw new PermanentStorageException(ex);
        } catch (MutationsRejectedException ex) {
            logger.error("Can't set configuration property " + tableName, ex);
            throw new TemporaryStorageException(ex);   
        }
    }
}
