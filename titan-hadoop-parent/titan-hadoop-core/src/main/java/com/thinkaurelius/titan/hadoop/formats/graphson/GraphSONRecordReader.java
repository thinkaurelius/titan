package com.thinkaurelius.titan.hadoop.formats.graphson;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.VertexQueryFilter;

import com.thinkaurelius.titan.hadoop.formats.util.TitanGraphOutputMapReduce;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphSONRecordReader extends RecordReader<NullWritable, FaunusVertex> {

    private final LineRecordReader lineRecordReader;
    private final VertexQueryFilter vertexQuery;
    private FaunusVertex vertex = null;
    private HadoopGraphSONUtility graphsonUtil;

//    public enum Counters {
//        VERTEX_PARSE_EXCEPTIONS;
//    }

    private static final Logger log =
            LoggerFactory.getLogger(GraphSONRecordReader.class);

    public GraphSONRecordReader(VertexQueryFilter vertexQuery) {
        lineRecordReader = new LineRecordReader();
        this.vertexQuery = vertexQuery;
    }

    @Override
    public void initialize(final InputSplit genericSplit, final TaskAttemptContext context) throws IOException {
        lineRecordReader.initialize(genericSplit, context);
        org.apache.hadoop.conf.Configuration c = DEFAULT_COMPAT.getContextConfiguration(context);
        Configuration configuration = ModifiableHadoopConfiguration.of(c);
        graphsonUtil = new HadoopGraphSONUtility(configuration);
    }

    @Override
    public boolean nextKeyValue() throws IOException {

        while (lineRecordReader.nextKeyValue()) {
            try {
                vertex = graphsonUtil.fromJSON(lineRecordReader.getCurrentValue().toString());
                vertexQuery.defaultFilter(vertex);
                return true;
            } catch (Throwable t) {
                //DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTEX_PARSE_EXCEPTIONS, 1L);
                log.warn("Exception \"{}\" on JSON input: {}", t.getMessage(), lineRecordReader.getCurrentValue());
            }
        }

        return false;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public FaunusVertex getCurrentValue() {
        return vertex;
    }

    @Override
    public float getProgress() throws IOException {
        return lineRecordReader.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        lineRecordReader.close();
    }
}