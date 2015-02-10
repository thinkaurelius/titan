package com.thinkaurelius.titan.hadoop.formats.cassandra;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.hadoop.FaunusVertexQueryFilter;
import com.thinkaurelius.titan.hadoop.formats.util.TitanHadoopGraph;
import org.apache.cassandra.hadoop.ColumnFamilyRecordReader;

import java.io.IOException;

public class TitanCassandraWideRecordReader extends TitanCassandraRecordReader {

    private TitanHadoopGraph.VertexBuilder vb = null;

    public TitanCassandraWideRecordReader(final TitanCassandraInputFormat inputFormat,
                                          final FaunusVertexQueryFilter vertexQuery,
                                          final ColumnFamilyRecordReader reader) {
        super(inputFormat, vertexQuery, reader);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        // Consume columns on one row until end of iterator or a new key appears
        while (reader.nextKeyValue()) {
            StaticBuffer key = StaticArrayBuffer.of(reader.getCurrentKey().duplicate()); // TODO check and remove duplication
            Iterable<Entry> columns = new TitanCassandraHadoopGraph.CassandraMapIterable(reader.getCurrentValue());

            if (null == vb) {
                // Initialization on first iteration
                vb = graph.newVertexBuilder(configuration, key);
                Preconditions.checkArgument(vb.getKey().equals(key));
            } else if (!vb.getKey().equals(key)) {
                // Handle new row key
                vertex = vb.build();
                vertexQuery.filterRelationsOf(vertex);
                vb = graph.newVertexBuilder(configuration, key);
                // Vertex can be null if the system property representing lifecycle state (deletion) is set
                if (null != vertex)
                    return true;
                // Continue iterating if it was null
            }

            // Add (column, value) pairs from current row
            vb.addEntries(columns);
        }

        // Iterator exhausted: check whether an unfinished vertex is ready for construction
        if (null != vb) {
            vertex = vb.build();
            vertexQuery.filterRelationsOf(vertex);
            vb = null;
            // Vertex can be null if the system property representing lifecycle state (deletion) is set
            return null != vertex;
        }

        return false;
    }
}
