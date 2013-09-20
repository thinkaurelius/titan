package com.thinkaurelius.titan.diskstorage.accumulo.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;

import junit.framework.TestCase;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DefaultIteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public class ColumnRangeFilterTest extends TestCase {

    private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();

    private Key nkv(TreeMap<Key, Value> tm, String row, String cf, String cq, String val) {
        Key k = nk(row, cf, cq);
        tm.put(k, new Value(val.getBytes()));
        return k;
    }

    private Key nk(String row, String cf, String cq) {
        return new Key(new Text(row), new Text(cf), new Text(cq));
    }

    @Test
    public void test1() throws IOException {
        TreeMap<Key, Value> tm = new TreeMap<Key, Value>();

        Key k1 = nkv(tm, "row1", "cf1", "a", "x");
        Key k2 = nkv(tm, "row1", "cf1", "b", "y");
        Key k3 = nkv(tm, "row1", "cf2", "c", "z");

        ColumnRangeFilter cri = new ColumnRangeFilter();
        cri.describeOptions();

        IteratorSetting is = new IteratorSetting(1, ColumnRangeFilter.class);
        ColumnRangeFilter.setRange(is, (String) null, false, (String) null, false);
        assertTrue(cri.validateOptions(is.getOptions()));
        cri.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
        cri.deepCopy(new DefaultIteratorEnvironment());
        cri.seek(new Range(), EMPTY_COL_FAMS, false);
        
        assertTrue(cri.hasTop());
        assertTrue(cri.getTopKey().equals(k1));
        cri.next();
        cri.next();
        assertTrue(cri.hasTop());
        
        // -----------------------------------------------------
        is.clearOptions();
        ColumnRangeFilter.setRange(is, "c", false, null, false);
        assertTrue(cri.validateOptions(is.getOptions()));
        cri.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
        cri.seek(new Range(), EMPTY_COL_FAMS, false);

        assertFalse(cri.hasTop());
             
        // -----------------------------------------------------
        is.clearOptions();
        ColumnRangeFilter.setRange(is, "b", false, null, false);
        assertTrue(cri.validateOptions(is.getOptions()));
        cri.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
        cri.seek(new Range(), EMPTY_COL_FAMS, false);

        assertTrue(cri.hasTop());
        assertTrue(cri.getTopKey().equals(k3));
        cri.next();
        assertFalse(cri.hasTop());
        
        // -----------------------------------------------------
        is.clearOptions();
        ColumnRangeFilter.setRange(is, "b", true, null, false);
        assertTrue(cri.validateOptions(is.getOptions()));
        cri.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
        cri.seek(new Range(), EMPTY_COL_FAMS, false);

        assertTrue(cri.hasTop());
        assertTrue(cri.getTopKey().equals(k2));
        cri.next();
        assertTrue(cri.hasTop());
        
        // -----------------------------------------------------
        is.clearOptions();
        ColumnRangeFilter.setRange(is, null, false, "b", false);
        assertTrue(cri.validateOptions(is.getOptions()));
        cri.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
        cri.seek(new Range(), EMPTY_COL_FAMS, false);

        assertTrue(cri.hasTop());
        assertTrue(cri.getTopKey().equals(k1));
        cri.next();
        assertFalse(cri.hasTop());
        
        // -----------------------------------------------------
        is.clearOptions();
        ColumnRangeFilter.setRange(is, null, false, "b", true);
        assertTrue(cri.validateOptions(is.getOptions()));
        cri.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
        cri.seek(new Range(), EMPTY_COL_FAMS, false);

        assertTrue(cri.hasTop());
        assertTrue(cri.getTopKey().equals(k1));
        cri.next();
        assertTrue(cri.hasTop());
        
        // -----------------------------------------------------
        is.clearOptions();
        ColumnRangeFilter.setRange(is, "b", true, "c", false);
        assertTrue(cri.validateOptions(is.getOptions()));
        cri.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
        cri.seek(new Range(), EMPTY_COL_FAMS, false);

        assertTrue(cri.hasTop());
        assertTrue(cri.getTopKey().equals(k2));
        cri.next();
        assertFalse(cri.hasTop());
        
        // -----------------------------------------------------
        is.clearOptions();
        ColumnRangeFilter.setRange(is, "b", true, "c", true);
        assertTrue(cri.validateOptions(is.getOptions()));
        cri.init(new SortedMapIterator(tm), is.getOptions(), new DefaultIteratorEnvironment());
        cri.seek(new Range(), EMPTY_COL_FAMS, false);

        assertTrue(cri.hasTop());
        assertTrue(cri.getTopKey().equals(k2));
        cri.next();
        assertTrue(cri.hasTop());
    }
}