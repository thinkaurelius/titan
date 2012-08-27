package com.thinkaurelius.titan.graphdb.configuration;

import java.lang.reflect.InvocationTargetException;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.diskstorage.StorageManager;

public abstract class AbstractStorageManagerService<T extends StorageManager> implements StorageManagerFactory<T> {

    public abstract Class<T> getStorageManagerClass();

    public StorageManager createStorageManager(Configuration storageconfig) {
        final String clazzname = storageconfig.getString(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,
                GraphDatabaseConfiguration.STORAGE_BACKEND_DEFAULT);
        try {
            return getStorageManagerClass().getConstructor(Configuration.class).newInstance(storageconfig);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Configured storage manager does not have required constructor: "
                    + clazzname);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("Could not instantiate storage manager class " + clazzname, e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Could not instantiate storage manager class " + clazzname, e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Could not instantiate storage manager class " + clazzname, e);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Could not instantiate storage manager class " + clazzname, e);
        }
    }

}
