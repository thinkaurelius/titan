package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import cern.colt.map.OpenIntIntHashMap;
import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntOpenHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleBulkPlacementStrategy implements IDPlacementStrategy {

    private static final Logger log =
            LoggerFactory.getLogger(SimpleBulkPlacementStrategy.class);

    public static final String CONCURRENT_PARTITIONS_KEY = "num-partitions";
    public static final int CONCURRENT_PARTITIONS_DEFAULT = 10;

    private final Random random = new Random();

    private final IDManager idManager;
    private final int[] currentPartitions;
    private int lowerPartitionID = -1;
    private int partitionWidth = -1;
    private int idCeiling = -1;
//    private IntSet exhaustedPartitions = new IntHashSet(); NOT THREAD SAFE!!

    public SimpleBulkPlacementStrategy(int concurrentPartitions, IDManager idManager) {
        Preconditions.checkArgument(concurrentPartitions > 0);
        Preconditions.checkNotNull(idManager);
        currentPartitions = new int[concurrentPartitions];
        this.idManager=idManager;
    }

    public SimpleBulkPlacementStrategy(Configuration config, IDManager idManager) {
        this(config.getInt(CONCURRENT_PARTITIONS_KEY, CONCURRENT_PARTITIONS_DEFAULT),idManager);
    }

    private final int nextPartitionID() {
        return currentPartitions[random.nextInt(currentPartitions.length)];
    }

    private final void updateElement(int index) {
        Preconditions.checkArgument(lowerPartitionID >= 0 && partitionWidth > 0 && idCeiling > 0);
        Preconditions.checkArgument(index >= 0 && index < currentPartitions.length);
        currentPartitions[index] = (random.nextInt(partitionWidth) + lowerPartitionID) % idCeiling;
    }

    @Override
    public long getPartition(InternalElement vertex) {
        return nextPartitionID();
    }

    @Override
    public void getPartitions(Map<InternalVertex, PartitionAssignment> vertices) {
        IntIntMap partitionCounts = new IntIntOpenHashMap();
        for (InternalVertex v : vertices.keySet()) {
            Preconditions.checkArgument(v.isNew());
            for (Vertex ngh : v.getVertices(Direction.BOTH)) {
                if (!((TitanVertex)ngh).isNew()) {
                    long partitionid = idManager.getPartitionID(((TitanVertex)ngh).getID());
                    Preconditions.checkArgument(partitionid>=0 && partitionid<Integer.MAX_VALUE);
                    partitionCounts.put((int)partitionid,partitionCounts.get((int)partitionid)+1);
                }
            }
        }

        int partitionID=-1;
        if (partitionCounts.isEmpty()) {
            partitionID = nextPartitionID();
        } else {
            //Pick highest counting one
            int maxCount=0;
            for (IntCursor ic : partitionCounts.keys()) {
                int p = ic.value;
                int c = partitionCounts.get(p);
                if (c>maxCount) {
                    partitionID=p;
                    maxCount=c;
                }
            }
            log.debug("Highest scoring partition {} of {} partitions",partitionID,partitionCounts.size());
        }
        Preconditions.checkArgument(partitionID>=0);
        for (Map.Entry<InternalVertex, PartitionAssignment> entry : vertices.entrySet()) {
            entry.setValue(new SimplePartitionAssignment(partitionID));
        }
    }

    @Override
    public boolean supportsBulkPlacement() {
        return true;
    }

    @Override
    public void setLocalPartitionBounds(int lowerID, int upperID, int idLimit) {
        Preconditions.checkArgument(idLimit > 0);
        Preconditions.checkArgument(lowerID >= 0 && lowerID < idLimit, lowerID);
        Preconditions.checkArgument(upperID >= 0 && upperID <= idLimit, upperID);
        lowerPartitionID = lowerID;
        idCeiling = idLimit;
        if (lowerID < upperID) partitionWidth = upperID - lowerPartitionID;
        else partitionWidth = (idLimit - lowerID) + upperID;
        Preconditions.checkArgument(partitionWidth > 0, partitionWidth);
        for (int i = 0; i < currentPartitions.length; i++) {
            updateElement(i);
        }
    }

    @Override
    public void exhaustedPartition(int partitionID) {
        boolean found = false;
        for (int i = 0; i < currentPartitions.length; i++) {
            if (currentPartitions[i] == partitionID) {
                updateElement(i);
                found = true;
            }
        }
//        if (found) {
//            exhaustedPartitions.add(partitionID);
//        } else {
//            if (!exhaustedPartitions.contains(partitionID))
//                log.error("Non-existant partition exhausted {} in {}", partitionID, Arrays.toString(currentPartitions));
//        }
    }
}
