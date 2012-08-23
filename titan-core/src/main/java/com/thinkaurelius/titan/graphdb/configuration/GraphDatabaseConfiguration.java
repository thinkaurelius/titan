package com.thinkaurelius.titan.graphdb.configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import javax.annotation.Nullable;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.graphdb.blueprints.BlueprintsDefaultTypeMaker;
import com.thinkaurelius.titan.graphdb.database.idassigner.SimpleVertexIDAssigner;
import com.thinkaurelius.titan.graphdb.database.idassigner.VertexIDAssigner;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;

/**
 * Provides functionality to configure a {@link com.thinkaurelius.titan.core.TitanGraph} INSTANCE.
 *
 * 
 * A graph database configuration is uniquely associated with a graph database and must not be used for multiple
 * databases. 
 * 
 * After a graph database has been initialized with respect to a configuration, some parameters of graph database
 * configuration may no longer be modifiable.
 * 
 * 
 * @author  Matthias Br&ouml;cheler (me@matthiasb.com);
 * 
 * 
 * 
 *
 */
public class GraphDatabaseConfiguration {

    private static final Map<String,DefaultTypeMaker> preregisteredAutoType = new HashMap<String,DefaultTypeMaker>() {{
        put("blueprints", BlueprintsDefaultTypeMaker.INSTANCE);
    }};


    public static final String STORAGE_NAMESPACE = "storage";

    /**
     * Storage directory for those storage backends that require local storage
     */
    public static final String STORAGE_DIRECTORY_KEY = "directory";

    /**
     * Define the storage backed to use for persistence
     */
    public static final String STORAGE_BACKEND_KEY = "backend";
    public static final String STORAGE_BACKEND_DEFAULT = "local";

    /**
     * Specifies whether write operations are supported
     */
    public static final String STORAGE_READONLY_KEY = "read-only";
    public static final boolean STORAGE_READONLY_DEFAULT = false;

    /**
     * Enables batch loading which improves write performance but assumes that only one thread is interacting with
     * the graph
     */
    public static final String STORAGE_BATCH_KEY = "batch-loading";
    public static final boolean STORAGE_BATCH_DEFAULT = false;

    /**
     * Enables transactions on storage backends that support them
     */
    public static final String STORAGE_TRANSACTIONAL_KEY = "transactions";
    public static final boolean STORAGE_TRANSACTIONAL_DEFAULT = true;




    /**
     * Buffers graph mutations locally up to the specified number before persisting them against the storage backend.
     * Set to 0 to disable buffering. Buffering is disabled automatically if the storage backend does not support buffered mutations.
     */
    public static final String BUFFER_SIZE_KEY = "buffer-size";
    public static final int BUFFER_SIZE_DEFAULT = 1024;

    /**
     * If flush ids is enabled, vertices and edges are assigned ids immediately upon creation. If not, then ids are only
     * assigned when the transaction is committed.
     */
    public static final String FLUSH_IDS_KEY = "flush-ids";
    public static final boolean FLUSH_IDS_DEFAULT = true;

    /**
     * Configures the {@link DefaultTypeMaker} to be used by this graph. If left empty, automatic creation of types
     * is disabled.
     */
    public static final String AUTO_TYPE_KEY = "autotype";
    public static final String AUTO_TYPE_DEFAULT = "blueprints";
    
    private static final String ID_RANDOMIZER_BITS_KEY = "id-random-bits";
    private static final int ID_RANDOMIZER_BITS_DEFAULT = 0;

    /**
     * The number of milliseconds the system waits for an id block application to be acknowledged by the storage backend.
     * Also, the time waited after the application before verifying that the application was successful.
     */
    public static final String IDAUTHORITY_WAIT_MS_KEY = "idauthority-wait-time";
    public static final long IDAUTHORITY_WAIT_MS_DEFAULT = 300;
    /**
     * Number of times the system attempts to acquire a unique id block before giving up and throwing an exception.
     */
    public static final String IDAUTHORITY_RETRY_COUNT_KEY = "idauthority-retries";
    public static final int IDAUTHORITY_RETRY_COUNT_DEFAULT = 3;
    /**
     * Size of the block to be acquired. Larger block sizes require fewer block applications but also leave a larger
     * fraction of the id pool occupied and potentially lost. For write heavy applications, larger block sizes should
     * be chosen.
     */
    public static final String IDAUTHORITY_BLOCK_SIZE_KEY = "idauthority-block-size";
    public static final int IDAUTHORITY_BLOCK_SIZE_DEFAULT = 100000;


    /**
     *  A unique identifier for the machine running the @TitanGraph@ instance.
     *  It must be ensured that no other machine accessing the storage backend can have the same identifier.
     */
    public static final String INSTANCE_RID_RAW_KEY = "machine-id";
    /**
     * A locally unique identifier for a particular @TitanGraph@ instance. This only needs to be configured
     * when multiple @TitanGraph@ instances are running on the same machine. A unique machine specific appendix
     * guarantees a globally unique identifier.
     */
    public static final String INSTANCE_RID_SHORT_KEY = "machine-id-appendix";

    /**
     * Number of times the system attempts to acquire a lock before giving up and throwing an exception.
     */
    public static final String LOCK_RETRY_COUNT = "lock-retries";
    public static final int LOCK_RETRY_COUNT_DEFAULT = 3;
    /**
     * The number of milliseconds the system waits for a lock application to be acknowledged by the storage backend.
     * Also, the time waited at the end of all lock applications before verifying that the applications were successful.
     * This value should be a small multiple of the average consistent write time.
     */
    public static final String LOCK_WAIT_MS = "lock-wait-time";
    public static final long LOCK_WAIT_MS_DEFAULT = 100;
    /**
     * Number of milliseconds after which a lock is considered to have expired. Lock applications that were not released
     * are considered expired after this time and released.
     * This value should be larger than the maximum time a transaction can take in order to guarantee that no correctly
     * held applications are expired pre-maturely and as small as possible to avoid dead lock.
     */
    public static final String LOCK_EXPIRE_MS = "lock-expiry-time";
    public static final long LOCK_EXPIRE_MS_DEFAULT = 300 * 1000;

    public static final String ATTRIBUTE_NAMESPACE = "attributes";

    public static final String ATTRIBUTE_ALLOW_ALL_SERIALIZABLE_KEY = "allow-all";
    public static final boolean ATTRIBUTE_ALLOW_ALL_SERIALIZABLE_DEFAULT = true;
    private static final String ATTRIBUTE_PREFIX = "attribute";
    private static final String SERIALIZER_PREFIX = "serializer";

    private static final String STORAGE_EDGESTORE_DEFAULT = "edgestore";
    private static final String STORAGE_PROPERTYINDEX_DEFAULT = "propertyindex";
    
    
    private final Configuration configuration;
    
    private boolean readOnly;
    private boolean flushIDs;
    private boolean batchLoading;
    private DefaultTypeMaker defaultTypeMaker;
    
    
    public GraphDatabaseConfiguration(Configuration config) {
        Preconditions.checkNotNull(config);
        this.configuration = config;
        preLoadConfiguration();
    }

    public GraphDatabaseConfiguration(File dirOrFile) {
        Preconditions.checkNotNull(dirOrFile,"Need to specify a directory");
        try {
            if (dirOrFile.isFile()) {
                configuration = new PropertiesConfiguration(dirOrFile);
            } else {
                if (!dirOrFile.isDirectory()) {
                    if (!dirOrFile.mkdirs()) {
                        throw new IllegalArgumentException("Could not create directory: " + dirOrFile);
                    }
                }
                Preconditions.checkArgument(dirOrFile.isDirectory());
                configuration = new BaseConfiguration();
                configuration.setProperty(keyInNamespace(STORAGE_NAMESPACE,STORAGE_DIRECTORY_KEY),dirOrFile.getAbsolutePath());
            }
        } catch (ConfigurationException e) {
                throw new IllegalArgumentException("Could not load configuration at: " + dirOrFile,e);
        }
        preLoadConfiguration();
    }

    public GraphDatabaseConfiguration(String dirOrFile) {
        this(new File(dirOrFile));
    }

    private void preLoadConfiguration() {
        Configuration storageConfig = configuration.subset(STORAGE_NAMESPACE);
        readOnly = storageConfig.getBoolean(STORAGE_READONLY_KEY,STORAGE_READONLY_DEFAULT);
        flushIDs = configuration.getBoolean(FLUSH_IDS_KEY,FLUSH_IDS_DEFAULT);
        batchLoading = storageConfig.getBoolean(STORAGE_BATCH_KEY,STORAGE_BATCH_DEFAULT);
        defaultTypeMaker = preregisteredAutoType.get(configuration.getString(AUTO_TYPE_KEY, AUTO_TYPE_DEFAULT));
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean hasFlushIDs() {
        return flushIDs;
    }

    public boolean isBatchLoading() {
        return batchLoading;
    }

    public DefaultTypeMaker getDefaultTypeMaker() {
        return defaultTypeMaker;
    }

    public boolean hasBufferMutations() {
        return configuration.getInt(BUFFER_SIZE_KEY, BUFFER_SIZE_DEFAULT)>0;
    }
    
    public int getBufferSize() {
        int size = configuration.getInt(BUFFER_SIZE_KEY, BUFFER_SIZE_DEFAULT);
        Preconditions.checkArgument(size>0,"Buffer size must be positive");
        return size;
    }
    

    public static List<RegisteredAttributeClass<?>> getRegisteredAttributeClasses(Configuration config) {
        List<RegisteredAttributeClass<?>> all = new ArrayList<RegisteredAttributeClass<?>>();
        Iterator<String> iter = config.getKeys();
        while (iter.hasNext()) {
            String key = iter.next();
            if (!key.startsWith(ATTRIBUTE_PREFIX)) continue;
            try {
                int position = Integer.parseInt(key.substring(ATTRIBUTE_PREFIX.length()));
                Class<?> clazz = null;
                AttributeSerializer<?> serializer = null; 
                String classname = config.getString(key);
                try {
                    clazz = Class.forName(classname);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("Could not find attribute class" + classname);
                }
                Preconditions.checkNotNull(clazz);

                if (config.containsKey(SERIALIZER_PREFIX+position)) {
                    String serializername = config.getString(SERIALIZER_PREFIX+position);
                    try {
                        Class sclass = Class.forName(serializername);
                        serializer = (AttributeSerializer)sclass.newInstance();
                    } catch (ClassNotFoundException e) {
                        throw new IllegalArgumentException("Could not find serializer class" + serializername);
                    } catch (InstantiationException e) {
                        throw new IllegalArgumentException("Could not instantiate serializer class" + serializername,e);
                    } catch (IllegalAccessException e) {
                        throw new IllegalArgumentException("Could not instantiate serializer class" + serializername,e);
                    }
                }
                RegisteredAttributeClass reg = new RegisteredAttributeClass(clazz,serializer,position);
                for (int i=0;i<all.size();i++) {
                    if (all.get(i).equals(reg)) {
                        throw new IllegalArgumentException("Duplicate attribute registration: " + all.get(i) + " and " + reg);
                    }
                }
                all.add(reg);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid attribute definition: " + key,e);
            }
        }
        Collections.sort(all);
        return all;
    }

    public VertexIDAssigner getIDAssigner(StorageManager storage) {
        IDManager idmanager = new IDManager();
        return new SimpleVertexIDAssigner(idmanager, storage,
                configuration.getInt(ID_RANDOMIZER_BITS_KEY,ID_RANDOMIZER_BITS_DEFAULT));
    }

    public String getStorageManagerDescription() {
        Configuration storageconfig = configuration.subset(STORAGE_NAMESPACE);
        String clazzname = storageconfig.getString(STORAGE_BACKEND_KEY,STORAGE_BACKEND_DEFAULT);
        if (storageconfig.containsKey(StorageManager.HOSTNAME_KEY)) {
            return clazzname + ":" + storageconfig.getString(StorageManager.HOSTNAME_KEY);
        } else {
            return clazzname + ":" + storageconfig.getString(STORAGE_DIRECTORY_KEY);
        }
    }
    

    
    public StorageManager getStorageManager() {
        final Configuration storageconfig = configuration.subset(STORAGE_NAMESPACE);
        final String storageManagerType = storageconfig.getString(STORAGE_BACKEND_KEY, STORAGE_BACKEND_DEFAULT);

        @SuppressWarnings("rawtypes")
        final ServiceLoader<StorageManagerFactory> serviceLoader = ServiceLoader.load(StorageManagerFactory.class);
        @SuppressWarnings("rawtypes")
        final Optional<StorageManagerFactory> optional = Iterables.tryFind(serviceLoader,
                new Predicate<StorageManagerFactory>() {

                    @Override
                    public boolean apply(@Nullable StorageManagerFactory input) {
                        return input.getName().equals(storageManagerType);
                    }
                });
        if (!optional.isPresent()) {
            throw new IllegalArgumentException("Could not find storage manager class " + storageManagerType);
        }

        return optional.get().createStorageManager(storageconfig);
    }
    
    public OrderedKeyColumnValueStore getEdgeStore(StorageManager m) {
        String name = STORAGE_EDGESTORE_DEFAULT;
        return m.openDatabase(name);
    }
    
    public OrderedKeyColumnValueStore getPropertyIndex(StorageManager m) {
        String name = STORAGE_PROPERTYINDEX_DEFAULT;
        return m.openDatabase(name);
    }
    
    public Serializer getSerializer() {
        Configuration config = configuration.subset(ATTRIBUTE_NAMESPACE);
        Serializer serializer = new KryoSerializer(config.getBoolean(ATTRIBUTE_ALLOW_ALL_SERIALIZABLE_KEY,ATTRIBUTE_ALLOW_ALL_SERIALIZABLE_DEFAULT));
        for (RegisteredAttributeClass<?> clazz : getRegisteredAttributeClasses(config)) {
            clazz.registerWith(serializer);
        }
        return serializer;
    }

    public boolean hasSerializeAll() {
        return configuration.subset(ATTRIBUTE_NAMESPACE).getBoolean(ATTRIBUTE_ALLOW_ALL_SERIALIZABLE_KEY,ATTRIBUTE_ALLOW_ALL_SERIALIZABLE_DEFAULT);
    }

    private static final String keyInNamespace(String namespace, String key) {
        return namespace + "." + key;
    }

    
    /* ----------------------------------------
     Methods for writing/reading config files
    -------------------------------------------*/
    
    /***
     * Returns the home directory for the graph database initialized in this configuration
     * 
     * @return Home directory for this graph database configuration
     */
    public File getHomeDirectory() {
        if (!configuration.containsKey(keyInNamespace(STORAGE_NAMESPACE,STORAGE_DIRECTORY_KEY)))
            throw new UnsupportedOperationException("No home directory specified");
        File dir = new File(configuration.getString(keyInNamespace(STORAGE_NAMESPACE,STORAGE_DIRECTORY_KEY)));
        Preconditions.checkArgument(dir.isDirectory(),"Not a directory");
        return dir;
    }

    //TODO: which of the following methods are really needed

    /***
     * Returns the home directory path for the graph database initialized in this configuration
     * 
     * @return Home directory path for this graph database configuration
     */
    public String getHomePath() {
        return getPath(getHomeDirectory());
    }

    private static File getSubDirectory(String base, String sub) {
        File subdir = new File(base, sub);
        if (!subdir.exists()) {
            if (!subdir.mkdir()) {
                throw new IllegalArgumentException("Cannot create subdirectory: " + sub);
            }
        }
        assert subdir.exists() && subdir.isDirectory();
        return subdir;
    }

    private static String getFileName(String dir, String file) {
        if (!dir.endsWith(File.separator)) dir = dir + File.separator;
        return dir + file;
    }

    public static String getPath(File dir) {
        return dir.getAbsolutePath()+File.separator;
    }


    static boolean existsFile(String file) {
        return (new File(file)).isFile();
    }

    static PropertiesConfiguration getPropertiesConfig(String file) {
        PropertiesConfiguration config = new PropertiesConfiguration();
        if (existsFile(file)) {
            try {
                config.load(file);
            } catch (ConfigurationException e) {
                throw new IllegalArgumentException("Cannot load existing configuration file: " + e.getMessage());
            }
        }
        config.setFileName(file);
        config.setAutoSave(true);
        return config;
    }
}