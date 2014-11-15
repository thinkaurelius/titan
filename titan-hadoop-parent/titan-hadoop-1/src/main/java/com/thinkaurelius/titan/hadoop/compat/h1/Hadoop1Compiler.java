package com.thinkaurelius.titan.hadoop.compat.h1;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.graphdb.configuration.TitanConstants;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.HadoopGraph;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.compat.HadoopCompiler;
import com.thinkaurelius.titan.hadoop.config.HybridConfigured;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.config.job.JobClasspathConfigurer;
import com.thinkaurelius.titan.hadoop.config.job.JobClasspathConfigurers;
import com.thinkaurelius.titan.hadoop.formats.FormatTools;
import com.thinkaurelius.titan.hadoop.formats.JobConfigurationFormat;
import com.thinkaurelius.titan.hadoop.hdfs.NoSideEffectFilter;

import com.thinkaurelius.titan.hadoop.mapreduce.AbstractHadoopCompiler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
//
//    public void composeJobs() throws IOException {
//
//        if (this.jobs.size() == 0) {
//            return;
//        }
//
//        if (getTitanConf().get(TitanHadoopConfiguration.PIPELINE_TRACK_PATHS))
//            logger.warn("Path tracking is enabled for this Titan/Hadoop job (space and time expensive)");
//        if (getTitanConf().get(TitanHadoopConfiguration.PIPELINE_TRACK_STATE))
//            logger.warn("State tracking is enabled for this Titan/Hadoop job (full deletes not possible)");
//
//        JobClasspathConfigurer cpConf = JobClasspathConfigurers.get(graph.getConf().get(Hadoop1Compat.CFG_JOB_JAR), JOB_JAR);
//
//        // Create temporary job data directory on the filesystem
//        Path tmpPath = graph.getJobDir();
//        final FileSystem fs = FileSystem.get(graph.getConf());
//        fs.mkdirs(tmpPath);
//        logger.debug("Created " + tmpPath + " on filesystem " + fs);
//        final String jobTmp = tmpPath.toString() + "/" + Tokens.JOB;
//        logger.debug("Set jobDir=" + jobTmp);
//
//        //////// CHAINING JOBS TOGETHER
//
//        logger.info("Compiled Titan-Hadoop task to " + this.jobs.size() + " MapReduce job(s)");
//
//        for (int i = 0; i < this.jobs.size(); i++) {
//            final Job job = this.jobs.get(i);
//            for (ConfigOption<Boolean> c : Arrays.asList(TitanHadoopConfiguration.PIPELINE_TRACK_PATHS, TitanHadoopConfiguration.PIPELINE_TRACK_STATE)) {
//                ModifiableHadoopConfiguration jobFaunusConf = ModifiableHadoopConfiguration.of(job.getConfiguration());
//                jobFaunusConf.set(c, getTitanConf().get(c));
//            }
//            FileOutputFormat.setOutputPath(job, new Path(jobTmp + "-" + i));
//            cpConf.configure(job);
//
//            // configure job inputs
//            if (i == 0) {
//                job.setInputFormatClass(this.graph.getGraphInputFormat());
//                if (FileInputFormat.class.isAssignableFrom(this.graph.getGraphInputFormat())) {
//                    FileInputFormat.setInputPaths(job, this.graph.getInputLocation());
//                    FileInputFormat.setInputPathFilter(job, NoSideEffectFilter.class);
//                }
//            } else {
//                job.setInputFormatClass(INTERMEDIATE_INPUT_FORMAT);
//                FileInputFormat.setInputPaths(job, new Path(jobTmp + "-" + (i - 1)));
//                FileInputFormat.setInputPathFilter(job, NoSideEffectFilter.class);
//            }
//
//            // configure job outputs
//            if (i == this.jobs.size() - 1) {
//                LazyOutputFormat.setOutputFormatClass(job, this.graph.getGraphOutputFormat());
//                MultipleOutputs.addNamedOutput(job, Tokens.SIDEEFFECT, this.graph.getSideEffectOutputFormat(), job.getOutputKeyClass(), job.getOutputKeyClass());
//                MultipleOutputs.addNamedOutput(job, Tokens.GRAPH, this.graph.getGraphOutputFormat(), NullWritable.class, FaunusVertex.class);
//            } else {
//                LazyOutputFormat.setOutputFormatClass(job, INTERMEDIATE_OUTPUT_FORMAT);
//                MultipleOutputs.addNamedOutput(job, Tokens.SIDEEFFECT, this.graph.getSideEffectOutputFormat(), job.getOutputKeyClass(), job.getOutputKeyClass());
//                MultipleOutputs.addNamedOutput(job, Tokens.GRAPH, INTERMEDIATE_OUTPUT_FORMAT, NullWritable.class, FaunusVertex.class);
//            }
//
//            logger.info("[Job " + (i + 1) + "/" + jobs.size() + "] " + job.getJobName());
//        }
//    }
//
//    @Override
//    public int run(final String[] args) throws Exception {
//
//        final FileSystem fs = FileSystem.get(getConf());
//
//        if (null != graph.getJobDir() &&
//            graph.getJobDirOverwrite() &&
//            fs.exists(graph.getJobDir())) {
//            fs.delete(graph.getJobDir(), true);
//        }
//
//        composeJobs();
//
//        final String jobTmp = graph.getJobDir().toString() + "/" + Tokens.JOB;
//
//        for (int i = 0; i < jobs.size(); i++) {
//            final Job job = jobs.get(i);
//            try {
//                ((JobConfigurationFormat) (FormatTools.getBaseOutputFormatClass(job).newInstance())).updateJob(job);
//            } catch (final Exception e) {
//            }
//            logger.info("Executing [Job " + (i + 1) + "/" + jobs.size() + "]: " + job.getJobName());
//            logger.debug("Map output key class: " + job.getMapOutputKeyClass());
//            logger.debug("Map output val class: " + job.getMapOutputValueClass());
//            logger.debug("Job output key class: " + job.getOutputKeyClass());
//            logger.debug("Job output val class: " + job.getOutputValueClass());
//            boolean success = job.waitForCompletion(true);
//            if (i > 0) {
//                Preconditions.checkNotNull(jobTmp);
//                final Path path = new Path(jobTmp + "-" + (i - 1));
//                // delete previous intermediate graph data
//                for (final FileStatus temp : fs.globStatus(new Path(path.toString() + "/" + Tokens.GRAPH + "*"))) {
//                    logger.debug("Deleting temp data location: " + temp.getPath());
//                    fs.delete(temp.getPath(), true);
//                }
//                // delete previous intermediate graph data
//                for (final FileStatus temp : fs.globStatus(new Path(path.toString() + "/" + Tokens.PART + "*"))) {
//                    logger.debug("Deleting temp data location: " + temp.getPath());
//                    fs.delete(temp.getPath(), true);
//                }
//            }
//            if (!success) {
//                logger.error("Titan/Hadoop job error -- remaining MapReduce jobs have been canceled");
//                return -1;
//            }
//        }
//        return 0;
//    }
}