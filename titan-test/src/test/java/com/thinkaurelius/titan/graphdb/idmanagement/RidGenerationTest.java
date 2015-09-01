package com.thinkaurelius.titan.graphdb.idmanagement;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import org.junit.Test;

import com.google.common.base.Preconditions;

public class RidGenerationTest {

    @Test
    public void testThreadBasedRidGeneration() throws InterruptedException {
        Configuration c = Configuration.EMPTY;
        int n = 8;
        Preconditions.checkArgument(1 < n); // n <= 1 is useless
        Collection<byte[]> rids = Collections.synchronizedSet(new HashSet<byte[]>());
        RidThread[] threads = new RidThread[n];
        
        for (int i = 0; i < n; i++) {
            threads[i] = new RidThread(rids, c);
        }
        
        for (int i = 0; i < n; i++) {
            threads[i].start();
        }
        
        for (int i = 0; i < n; i++) {
            threads[i].join();
        }
        //TODO: rewrite test case in terms of GraphDatabaseConfiguration
        //assertEquals(n, rids.size());
    }
    
    private static class RidThread extends Thread {
        
        private final Collection<byte[]> rids;
        private final Configuration c;
        
        private RidThread(Collection<byte[]> rids, Configuration c) {
            this.rids = rids;
            this.c = c;
        }
        
        @Override
        public void run() {
            //rids.add(DistributedStoreManager.getRid(c));
        }
    };
}
