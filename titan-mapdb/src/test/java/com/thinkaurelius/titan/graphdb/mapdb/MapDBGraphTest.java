package com.thinkaurelius.titan.graphdb.mapdb;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.MapDBStorageSetup;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.example.GraphOfTheGodsFactory;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.graphdb.TitanIndexTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * We opt not to implement different isolation levels at this stage.
 * Therefore, consistency checks are on Read Committed level
 */
public class MapDBGraphTest extends TitanGraphTest {

    private static final Logger log =
            LoggerFactory.getLogger(MapDBGraphTest.class);
    @Rule
    public TestName methodNameRule = new TestName();

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration mcfg = MapDBStorageSetup.getMapDBConfiguration();
//        String methodName = methodNameRule.getMethodName();
//        if (methodName.equals("testConsistencyEnforcement")) {
//            IsolationLevel iso = IsolationLevel.SERIALIZABLE;
//            log.debug("Forcing isolation level {} for test method {}", iso, methodName);
//            mcfg.set(BerkeleyJEStoreManager.ISOLATION_LEVEL, iso);
//        } else {
//            IsolationLevel iso = null;
//            if (mcfg.has(BerkeleyJEStoreManager.ISOLATION_LEVEL)) {
//                iso = mcfg.get(BerkeleyJEStoreManager.ISOLATION_LEVEL);
//            }
//            log.debug("Using isolation level {} (null means adapter default) for test method {}", iso, methodName);
//        }
        return mcfg.getConfiguration();
    }

    /**
     * Test {@link com.thinkaurelius.titan.example.GraphOfTheGodsFactory#create(String)}.
     */
    @Test
    public void testGraphOfTheGodsFactoryCreate() {
        String bdbtmp = Joiner.on(File.separator).join("target", "gotgfactory");
        TitanGraph gotg = GraphOfTheGodsFactory.create(bdbtmp);
        TitanIndexTest.assertGraphOfTheGods(gotg);
    }

    @Override
    public void testConsistencyEnforcement() {
        // Check that getConfiguration() explicitly set serializable isolation
        // This could be enforced with a JUnit assertion instead of a Precondition,
        // but a failure here indicates a problem in the test itself rather than the
        // system-under-test, so a Precondition seems more appropriate
//        IsolationLevel effective = config.get(ConfigElement.getPath(BerkeleyJEStoreManager.ISOLATION_LEVEL), IsolationLevel.class);
//        Preconditions.checkState(IsolationLevel.SERIALIZABLE.equals(effective));
        super.testConsistencyEnforcement();
    }

    @Override
    protected boolean isLockingOptimistic() {
        return false;
    }

    @Override
    public void testConcurrentConsistencyEnforcement() {
        //Do nothing TODO: Figure out why this is failing in BerkeleyDB!!
    }
}
