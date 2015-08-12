package com.thinkaurelius.titan.graphdb.idmanagement;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.PartitionIDRange;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PartitionIDRangeTest {


    @Test
    public void basicIDRangeTest() {
        PartitionIDRange pr;

        for (int[] bounds : new int[][]{{0,16},{5,5},{9,9},{0,0}}) {
            pr = new PartitionIDRange(bounds[0], bounds[1], 16);
            Set<Integer> allIds = Sets.newHashSet(Arrays.asList(ArrayUtils.toObject(pr.getAllContainedIDs())));
            assertEquals(16, allIds.size());
            for (int i = 0; i < 16; i++) {
                assertTrue(allIds.contains(i));
                assertTrue(pr.contains(i));
            }
            assertFalse(pr.contains(16));
            verifyRandomSampling(pr);
        }

        pr = new PartitionIDRange(13,2,16);
        assertTrue(pr.contains(15));
        assertTrue(pr.contains(1));
        assertEquals(5,pr.getAllContainedIDs().length);
        verifyRandomSampling(pr);

        pr = new PartitionIDRange(512,2,2048);
        assertEquals(2048-512+2,pr.getAllContainedIDs().length);
        verifyRandomSampling(pr);

        pr = new PartitionIDRange(512,1055,2048);
        assertEquals(1055-512,pr.getAllContainedIDs().length);
        verifyRandomSampling(pr);

        try {
            pr = new PartitionIDRange(0,5,4);
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            pr = new PartitionIDRange(5,3,4);
            fail();
        } catch (IllegalArgumentException e) {}
        try {
            pr = new PartitionIDRange(-1,3,4);
            fail();
        } catch (IllegalArgumentException e) {}
    }

    private void verifyRandomSampling(PartitionIDRange pr) {
        Set<Integer> allIds = Sets.newHashSet(Arrays.asList(ArrayUtils.toObject(pr.getAllContainedIDs())));
        /* Verify that the probability of NOT sampling the whole space is exceedingly small
        The probability of NOT sampling (with replacement) a single element from a set of x elements in T trials is:
         ((x-1)/x)^T
         Hence, an upper bound for not sampling any of the elements (assuming independence turning OR into +) is:
         x * ((x-1)/x)^T
        */
        double x = allIds.size();
        final int T = 300000;
        double failureSampleProb = x * Math.pow((x-1)/x,T);
        assertTrue(failureSampleProb<1e-12); //Make sure the failure prob is infinitisimally small

        Set<Integer> randomIds = Sets.newHashSet();
        for (int t=0;t<T;t++) {
            int id = pr.getRandomID();
            randomIds.add(id);
            assertTrue(allIds.contains(id));
        }
        assertEquals(allIds.size(),randomIds.size());

    }

    @Test
    public void convertIDRangesFromBits() {
        PartitionIDRange pr;

        for (int partitionBits : new int[]{0,1,4,16,5,7,2}) {
            pr = Iterables.getOnlyElement(PartitionIDRange.getGlobalRange(partitionBits));
            assertEquals(1<<partitionBits,pr.getUpperID());
            assertEquals(1<<partitionBits,pr.getAllContainedIDs().length);
            if (partitionBits<=10) verifyRandomSampling(pr);
        }

        try {
            PartitionIDRange.getGlobalRange(-1);
            fail();
        } catch (IllegalArgumentException e) {}
    }

    @Test
    public void convertIDRangesFromBuffers() {
        PartitionIDRange pr;

        pr = getPIR(2,2,6,3);
        assertEquals(2, pr.getAllContainedIDs().length);
        assertTrue(pr.contains(1));
        assertTrue(pr.contains(2));
        assertFalse(pr.contains(3));
        pr = getPIR(2,3,6,3);
        assertEquals(1, pr.getAllContainedIDs().length);
        assertFalse(pr.contains(1));
        assertTrue(pr.contains(2));
        assertFalse(pr.contains(3));
        pr = getPIR(4,2,6,3);
        assertEquals(8, pr.getAllContainedIDs().length);
        pr = getPIR(2,6,6,3);
        assertEquals(4,pr.getAllContainedIDs().length);
        pr = getPIR(2,7,7,3);
        assertEquals(4,pr.getAllContainedIDs().length);
        pr = getPIR(2,10,9,4);
        assertEquals(3,pr.getAllContainedIDs().length);
        pr = getPIR(2,5,15,4);
        assertEquals(1, pr.getAllContainedIDs().length);
        pr = getPIR(2,9,16,4);
        assertEquals(1, pr.getAllContainedIDs().length);
        assertTrue(pr.contains(3));

        assertNull(getPIR(2, 11, 12, 4));
        assertNull(getPIR(2, 5, 11, 4));
        assertNull(getPIR(2, 9, 12, 4));
        assertNull(getPIR(2, 9, 11, 4));
        assertNull(getPIR(2, 13, 15, 4));
        assertNull(getPIR(2, 13, 3, 4));


        pr = getPIR(2,15,14,4);
        assertEquals(3,pr.getAllContainedIDs().length);

        pr = getPIR(1,7,6,3);
        assertEquals(1, pr.getAllContainedIDs().length);
        assertTrue(pr.contains(0));
    }


    public static PartitionIDRange getPIR(int partitionBits, long lower, long upper, int bitwidth) {
        return Iterables.getOnlyElement(PartitionIDRange.getIDRanges(partitionBits, convert(lower, upper, bitwidth)), null);
    }

    public static List<KeyRange> convert(long lower, long upper, int bitwidth) {
        StaticBuffer lowerBuffer = BufferUtil.getLongBuffer(convert(lower, bitwidth));
        StaticBuffer upperBuffer = BufferUtil.getLongBuffer(convert(upper, bitwidth));
//        Preconditions.checkArgument(lowerBuffer.compareTo(upperBuffer) < 0, "%s vs %s",lowerBuffer,upperBuffer);
        return Lists.newArrayList(new KeyRange(lowerBuffer, upperBuffer));
    }

    public static long convert(long id, int bitwidth) {
        Preconditions.checkArgument(id>=0 && id<=(1<<bitwidth));
        return id<<(64-bitwidth);
    }

}
