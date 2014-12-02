package com.thinkaurelius.titan.hadoop.compat.h1;

import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Hadoop1Compiler extends AbstractHadoopCompiler implements HadoopCompiler {
// TODO tolerate null temporary SequenceFile path when only running a single job, like Hadoop2Compiler already does


    private static final String MAPRED_COMPRESS_MAP_OUTPUT = "mapred.compress.map.output";
    private static final String MAPRED_MAP_OUTPUT_COMPRESSION_CODEC = "mapred.map.output.compression.codec";

    public static final Logger logger = LoggerFactory.getLogger(Hadoop1Compiler.class);

    private final List<Class<? extends Mapper>> mapSequenceClasses = new ArrayList<Class<? extends Mapper>>();
    private Class<? extends WritableComparable> mapOutputKey = NullWritable.class;
    private Class<? extends WritableComparable> mapOutputValue = NullWritable.class;
    private Class<? extends WritableComparable> outputKey = NullWritable.class;
    private Class<? extends WritableComparable> outputValue = NullWritable.class;

    private Class<? extends Reducer> combinerClass = null;
    private Class<? extends WritableComparator> comparatorClass = null;
    private Class<? extends Reducer> reduceClass = null;

    static final String JOB_JAR = "titan-hadoop-1-" + TitanConstants.VERSION + "-job.jar";

    @Override
    protected Logger getLog() {
        return logger;
    }

    public Hadoop1Compiler(final HadoopGraph graph) {
        super(graph);
        addConfiguration(this.graph.getConf());
    }

    @Override
    protected String getMapReduceJarConfigKey() {
        return Hadoop1Compat.CFG_JOB_JAR;
    }

    @Override
    protected String getDefaultMapReduceJar() {
        return JOB_JAR;
    }

    private String toStringOfJob(final Class sequenceClass) {
        final List<String> list = new ArrayList<String>();
        for (final Class klass : this.mapSequenceClasses) {
            list.add(klass.getCanonicalName());
        }
        if (null != reduceClass) {
            list.add(this.reduceClass.getCanonicalName());
        }
        return sequenceClass.getSimpleName() + list.toString();
    }

    private String[] toStringMapSequenceClasses() {
        final List<String> list = new ArrayList<String>();
        for (final Class klass : this.mapSequenceClasses) {
            list.add(klass.getName());
        }
        return list.toArray(new String[list.size()]);
    }

    private void addConfiguration(final Configuration configuration) {
        for (final Map.Entry<String, String> entry : configuration) {
            this.getConf().set(entry.getKey(), entry.getValue());
        }
    }

    public void addMapReduce(final Class<? extends Mapper> mapper,
                             final Class<? extends Reducer> combiner,
                             final Class<? extends Reducer> reducer,
                             final Class<? extends WritableComparator> comparator,
                             final Class<? extends WritableComparable> mapOutputKey,
                             final Class<? extends WritableComparable> mapOutputValue,
                             final Class<? extends WritableComparable> reduceOutputKey,
                             final Class<? extends WritableComparable> reduceOutputValue,
                             final Configuration configuration) {

        this.addConfiguration(configuration);
        this.mapSequenceClasses.add(mapper);
        this.combinerClass = combiner;
        this.reduceClass = reducer;
        this.comparatorClass = comparator;
        this.mapOutputKey = mapOutputKey;
        this.mapOutputValue = mapOutputValue;
        this.outputKey = reduceOutputKey;
        this.outputValue = reduceOutputValue;
        this.completeSequence();
    }

    public void addMapReduce(final Class<? extends Mapper> mapper,
                             final Class<? extends Reducer> combiner,
                             final Class<? extends Reducer> reducer,
                             final Class<? extends WritableComparable> mapOutputKey,
                             final Class<? extends WritableComparable> mapOutputValue,
                             final Class<? extends WritableComparable> reduceOutputKey,
                             final Class<? extends WritableComparable> reduceOutputValue,
                             final Configuration configuration) {

        this.addConfiguration(configuration);
        this.mapSequenceClasses.add(mapper);
        this.combinerClass = combiner;
        this.reduceClass = reducer;
        this.mapOutputKey = mapOutputKey;
        this.mapOutputValue = mapOutputValue;
        this.outputKey = reduceOutputKey;
        this.outputValue = reduceOutputValue;
        this.completeSequence();
    }

    public void addMap(final Class<? extends Mapper> mapper,
                       final Class<? extends WritableComparable> mapOutputKey,
                       final Class<? extends WritableComparable> mapOutputValue,
                       final Configuration configuration) {

        this.addConfiguration(configuration);
        this.mapSequenceClasses.add(mapper);
        this.mapOutputKey = mapOutputKey;
        this.mapOutputValue = mapOutputValue;
        this.outputKey = mapOutputKey;
        this.outputValue = mapOutputValue;

    }

    public void completeSequence() {
        if (this.mapSequenceClasses.size() > 0) {
            this.getConf().setStrings(MapSequence.MAP_CLASSES, toStringMapSequenceClasses());
            final Job job;
            try {
                job = new Job(this.getConf(), this.toStringOfJob(MapSequence.class));
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }

            job.setJarByClass(HadoopCompiler.class);
            job.setMapperClass(MapSequence.Map.class);
            if (null != this.reduceClass) {
                job.setReducerClass(this.reduceClass);
                if (null != this.combinerClass)
                    job.setCombinerClass(this.combinerClass);
                // if there is a reduce task, compress the map output to limit network traffic
                if (null == job.getConfiguration().get(MAPRED_COMPRESS_MAP_OUTPUT, null))
                    job.getConfiguration().setBoolean(MAPRED_COMPRESS_MAP_OUTPUT, true);
                if (null == job.getConfiguration().get(MAPRED_MAP_OUTPUT_COMPRESSION_CODEC, null))
                    job.getConfiguration().setClass(MAPRED_MAP_OUTPUT_COMPRESSION_CODEC, DefaultCodec.class, CompressionCodec.class);
            } else {
                job.setNumReduceTasks(0);
            }

            job.setMapOutputKeyClass(this.mapOutputKey);
            job.setMapOutputValueClass(this.mapOutputValue);
            if (null != this.comparatorClass)
                job.setSortComparatorClass(this.comparatorClass);
            // else
            //   job.setSortComparatorClass(NullWritable.Comparator.class);
            job.setOutputKeyClass(this.outputKey);
            job.setOutputValueClass(this.outputValue);


            this.jobs.add(job);

            this.setConf(new Configuration());
            this.addConfiguration(this.graph.getConf());
            this.mapSequenceClasses.clear();
            this.combinerClass = null;
            this.reduceClass = null;
            this.comparatorClass = null;
        }
    }
}