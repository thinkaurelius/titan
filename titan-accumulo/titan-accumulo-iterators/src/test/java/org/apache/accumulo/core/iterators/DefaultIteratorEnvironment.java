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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.accumulo.core.file.map.MyMapFile.Reader;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

@SuppressWarnings("deprecation")
public class DefaultIteratorEnvironment implements IteratorEnvironment {

    AccumuloConfiguration conf;

    public DefaultIteratorEnvironment(AccumuloConfiguration conf) {
        this.conf = conf;
    }

    public DefaultIteratorEnvironment() {
        this.conf = AccumuloConfiguration.getDefaultConfiguration();
    }

    @Override
    public Reader reserveMapFileReader(String mapFileName) throws IOException {
        Configuration conf = CachedConfiguration.getInstance();
        FileSystem fs = FileSystem.get(conf);
        return new MyMapFile.Reader(fs, mapFileName, conf);
    }

    @Override
    public AccumuloConfiguration getConfig() {
        return conf;
    }

    @Override
    public IteratorScope getIteratorScope() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFullMajorCompaction() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerSideChannel(SortedKeyValueIterator<Key, Value> iter) {
        throw new UnsupportedOperationException();
    }
}
