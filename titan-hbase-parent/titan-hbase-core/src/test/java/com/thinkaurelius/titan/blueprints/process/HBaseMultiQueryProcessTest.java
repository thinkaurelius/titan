package com.thinkaurelius.titan.blueprints.process;

import com.thinkaurelius.titan.HBaseStorageSetup;
import com.thinkaurelius.titan.blueprints.HBaseMultiQueryGraphProvider;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import java.io.IOException;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = HBaseMultiQueryGraphProvider.class, graph = TitanGraph.class)
public class HBaseMultiQueryProcessTest {

    @BeforeClass
    public static void startHBase() {
        try {
            HBaseStorageSetup.startHBase();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void stopHBase() {
        // Workaround for https://issues.apache.org/jira/browse/HBASE-10312
        if (VersionInfo.getVersion().startsWith("0.96"))
            HBaseStorageSetup.killIfRunning();
    }
}
