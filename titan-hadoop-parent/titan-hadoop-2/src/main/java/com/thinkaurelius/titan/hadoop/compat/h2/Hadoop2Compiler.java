package com.thinkaurelius.titan.hadoop.compat.h2;

import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;
import com.thinkaurelius.titan.hadoop.config.HBaseAuthHelper;
import com.thinkaurelius.titan.hadoop.mapreduce.AbstractHadoopCompiler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Hadoop2Compiler extends AbstractHadoopCompiler implements HadoopCompiler {

    enum State {MAPPER, REDUCER, NONE}

    private static final String ARROW = " > ";
    private static final String MAPREDUCE_MAP_OUTPUT_COMPRESS = "mapreduce.map.output.compress";
    private static final String MAPREDUCE_MAP_OUTPUT_COMPRESS_CODEC = "mapreduce.map.output.compress.codec";

    public static final Logger logger = LoggerFactory.getLogger(Hadoop2Compiler.class);

    private State state = State.NONE;

    static final String JOB_JAR = "titan-hadoop-2-" + TitanConstants.VERSION + "-job.jar";

    private static final String MAPRED_JAR = "mapred.jar";

    @Override
    protected Logger getLog() {
        return logger;
    }

    public Hadoop2Compiler(final HadoopGraph graph) {
        super(graph);
    }

    @Override
    protected String getMapReduceJarConfigKey() {
        return MAPRED_JAR;
    }

    @Override
    protected String getDefaultMapReduceJar() {
        return JOB_JAR;
    }

    private String makeClassName(final Class klass) {
        return klass.getCanonicalName().replace(klass.getPackage().getName() + ".", "");
    }

    @Override
    public void addMapReduce(final Class<? extends Mapper> mapper,
                             final Class<? extends Reducer> combiner,
                             final Class<? extends Reducer> reducer,
                             final Class<? extends WritableComparable> mapOutputKey,
                             final Class<? extends WritableComparable> mapOutputValue,
                             final Class<? extends WritableComparable> reduceOutputKey,
                             final Class<? extends WritableComparable> reduceOutputValue,
                             final Configuration configuration) {
        this.addMapReduce(mapper, combiner, reducer, null, mapOutputKey, mapOutputValue, reduceOutputKey, reduceOutputValue, configuration);
    }

    @Override
    public void addMapReduce(final Class<? extends Mapper> mapper,
                             final Class<? extends Reducer> combiner,
                             final Class<? extends Reducer> reducer,
                             final Class<? extends WritableComparator> comparator,
                             final Class<? extends WritableComparable> mapOutputKey,
                             final Class<? extends WritableComparable> mapOutputValue,
                             final Class<? extends WritableComparable> reduceOutputKey,
                             final Class<? extends WritableComparable> reduceOutputValue,
                             final Configuration configuration) {

       Configuration mergedConf = overlayConfiguration(getConf(), configuration);

       try {
            final Job job;

            if (State.NONE == this.state || State.REDUCER == this.state) {
                // Create a new job with a reference to mergedConf
                job = Job.getInstance(mergedConf);
                job.setJobName(makeClassName(mapper) + ARROW + makeClassName(reducer));
                HBaseAuthHelper.setHBaseAuthToken(mergedConf, job);
                this.jobs.add(job);
            } else {
                job = this.jobs.get(this.jobs.size() - 1);
                job.setJobName(job.getJobName() + ARROW + makeClassName(mapper) + ARROW + makeClassName(reducer));
            }
            job.setNumReduceTasks(this.getConf().getInt("mapreduce.job.reduces", this.getConf().getInt("mapreduce.tasktracker.reduce.tasks.maximum", 1)));

            ChainMapper.addMapper(job, mapper, NullWritable.class, FaunusVertex.class, mapOutputKey, mapOutputValue, mergedConf);
            ChainReducer.setReducer(job, reducer, mapOutputKey, mapOutputValue, reduceOutputKey, reduceOutputValue, mergedConf);

            if (null != comparator)
                job.setSortComparatorClass(comparator);
            if (null != combiner)
                job.setCombinerClass(combiner);
            if (null == job.getConfiguration().get(MAPREDUCE_MAP_OUTPUT_COMPRESS, null))
                job.getConfiguration().setBoolean(MAPREDUCE_MAP_OUTPUT_COMPRESS, true);
            if (null == job.getConfiguration().get(MAPREDUCE_MAP_OUTPUT_COMPRESS_CODEC, null))
                job.getConfiguration().setClass(MAPREDUCE_MAP_OUTPUT_COMPRESS_CODEC, DefaultCodec.class, CompressionCodec.class);
            this.state = State.REDUCER;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

    }

    @Override
    public void addMap(final Class<? extends Mapper> mapper,
                       final Class<? extends WritableComparable> mapOutputKey,
                       final Class<? extends WritableComparable> mapOutputValue,
                       Configuration configuration) {

        Configuration mergedConf = overlayConfiguration(getConf(), configuration);

        try {
            final Job job;
            if (State.NONE == this.state) {
                // Create a new job with a reference to mergedConf
                job = Job.getInstance(mergedConf);
                job.setNumReduceTasks(0);
                job.setJobName(makeClassName(mapper));
                HBaseAuthHelper.setHBaseAuthToken(mergedConf, job);
                this.jobs.add(job);
            } else {
                job = this.jobs.get(this.jobs.size() - 1);
                job.setJobName(job.getJobName() + ARROW + makeClassName(mapper));
            }

            if (State.MAPPER == this.state || State.NONE == this.state) {
                ChainMapper.addMapper(job, mapper, NullWritable.class, FaunusVertex.class, mapOutputKey, mapOutputValue, mergedConf);
                /* In case no reducer is defined later for this job, set the job
                 * output k/v to match the mapper output k-v.  Output formats that
                 * care about their configured k-v classes (such as
                 * SequenceFileOutputFormat) require these to be set correctly lest
                 * they throw an exception at runtime.
                 *
                 * ChainReducer.setReducer overwrites these k-v settings, so if a
                 * reducer is added onto this job later, these settings will be
                 * overridden by the actual reducer's output k-v.
                 */
                job.setOutputKeyClass(mapOutputKey);
                job.setOutputValueClass(mapOutputValue);
                this.state = State.MAPPER;
                logger.info("Added mapper " + job.getJobName() + " via ChainMapper with output (" + mapOutputKey + "," + mapOutputValue + "); current state is " + state);
            } else {
                ChainReducer.addMapper(job, mapper, NullWritable.class, FaunusVertex.class, mapOutputKey, mapOutputValue, mergedConf);
                this.state = State.REDUCER;
                logger.info("Added mapper " + job.getJobName() + " via ChainReducer with output (" + mapOutputKey + "," + mapOutputValue + "); current state is " + state);
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void completeSequence() {
        // noop
    }

    private static Configuration overlayConfiguration(Configuration base, Configuration overrides) {
        Configuration mergedConf = new Configuration(base);
        final Iterator<Entry<String,String>> it = overrides.iterator();
        while (it.hasNext()) {
            Entry<String,String> ent = it.next();
            mergedConf.set(ent.getKey(), ent.getValue());
        }
        return mergedConf;
    }
}
