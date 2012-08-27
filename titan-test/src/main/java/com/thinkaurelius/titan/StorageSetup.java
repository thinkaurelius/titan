package com.thinkaurelius.titan;

import java.io.File;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.system.IOUtils;

public class StorageSetup {

	public static final String getHomeDir() {
        String homedir = System.getProperty("titan.testdir");
        if (null ==  homedir) {
             homedir = System.getProperty("java.io.tmpdir") + File.separator +  "titan-test";
        }
        File homefile = new File(homedir);
        if (!homefile.exists()) homefile.mkdirs();
        return homedir;
    }
    
    public static final File getHomeDirFile() {
        return new File(getHomeDir());
    }
	
	public static final void deleteHomeDir() {
		File homeDirFile = getHomeDirFile();
		// Make directory if it doesn't exist
		if (!homeDirFile.exists())
			homeDirFile.mkdirs();
		boolean success = IOUtils.deleteFromDirectory(homeDirFile);
        if (!success) throw new IllegalStateException("Could not remove " + homeDirFile) ;
	}

    public static Configuration getLocalStorageConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY,getHomeDir());
        return config;
    }
    
    

    


    //------

    public static Configuration getLocalGraphConfiguration() {
        BaseConfiguration config = new BaseConfiguration();
        config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE).addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY,getHomeDir());
        return config;
    }
}
