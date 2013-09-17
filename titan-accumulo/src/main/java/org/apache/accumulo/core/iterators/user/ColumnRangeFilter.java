/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.iterators.user;


import java.io.IOException;
import java.util.Map;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * A Filter that matches entries based on Java regular expressions.
 */
public class ColumnRangeFilter extends Filter {

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        ColumnRangeFilter result = (ColumnRangeFilter) super.deepCopy(env);
        result.minColumn = minColumn;
        result.minColInclusive = minColInclusive;
        result.maxColumn = maxColumn;
        result.maxColInclusive = maxColInclusive;
        return result;
    }
    public static final String MIN_COLUMN = "minColumn";
    public static final String MIN_COL_INCLUSIVE = "minColInclusive";
    public static final String MAX_COLUMN = "maxColumn";
    public static final String MAX_COL_INCLUSIVE = "maxColInclusive";
    private ByteSequence minColumn;
    private boolean minColInclusive;
    private ByteSequence maxColumn;
    private boolean maxColInclusive;

    @Override
    public boolean accept(Key key, Value value) {
        int cmpMin = -1;

        if (minColumn != null) {
            cmpMin = minColumn.compareTo(key.getColumnQualifierData());
        }

        if (cmpMin > 0 || (!minColInclusive && cmpMin == 0)) {
            return false;
        }

        if (maxColumn == null) {
            return true;
        }

        int cmpMax = maxColumn.compareTo(key.getColumnQualifierData());

        if (cmpMax < 0 || (!maxColInclusive && cmpMax == 0)) {
            return false;
        }

        return true;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (options.containsKey(MIN_COLUMN)) {
            minColumn = new ArrayByteSequence(options.get(MIN_COLUMN));
        } else {
            minColumn = null;
        }

        if (options.containsKey(MIN_COL_INCLUSIVE)) {
            minColInclusive = Boolean.parseBoolean(options.get(MIN_COL_INCLUSIVE));
        } else {
            minColInclusive = true;
        }

        if (options.containsKey(MAX_COLUMN)) {
            maxColumn = new ArrayByteSequence(options.get(MAX_COLUMN));
        } else {
            maxColumn = null;
        }

        if (options.containsKey(MAX_COL_INCLUSIVE)) {
            maxColInclusive = Boolean.parseBoolean(options.get(MAX_COL_INCLUSIVE));
        } else {
            maxColInclusive = false;
        }
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.setName("colrange");
        io.setDescription("The ColumnRangeFilter/Iterator allows you to filter for a range of column qualifiers");
        io.addNamedOption(MIN_COLUMN, "mininum column qualifier");
        io.addNamedOption(MIN_COL_INCLUSIVE, "minimum column inclusive");
        io.addNamedOption(MAX_COLUMN, "maximum column qualifier");
        io.addNamedOption(MAX_COL_INCLUSIVE, "maximum column inclusive");
        return io;
    }

    public static void setRange(IteratorSetting is, String minColumn, boolean minColInclusive,
            String maxColumn, boolean maxColInclusive) {
        if (minColumn != null && minColumn.length() > 0) {
            is.addOption(ColumnRangeFilter.MIN_COLUMN, minColumn);
        }
        if (!minColInclusive) {
            is.addOption(ColumnRangeFilter.MIN_COL_INCLUSIVE, "false");
        }
        if (maxColumn != null && maxColumn.length() > 0) {
            is.addOption(ColumnRangeFilter.MAX_COLUMN, maxColumn);
        }
        if (maxColInclusive) {
            is.addOption(ColumnRangeFilter.MAX_COL_INCLUSIVE, "true");
        }
    }
    
    public static void setRange(IteratorSetting is, byte[] minColumn, boolean minColInclusive,
            byte[] maxColumn, boolean maxColInclusive) {
        setRange(is, new String(minColumn), minColInclusive, new String(maxColumn), maxColInclusive);
    }
}
