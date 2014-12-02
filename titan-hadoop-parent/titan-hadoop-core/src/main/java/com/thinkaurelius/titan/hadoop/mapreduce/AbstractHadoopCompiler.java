package com.thinkaurelius.titan.hadoop.mapreduce;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractHadoopCompiler extends HybridConfigured implements HadoopCompiler {

    protected final HadoopGraph graph;
    protected final List<Job> jobs;

    private static final Class<? extends InputFormat> INTERMEDIATE_INPUT_FORMAT = SequenceFileInputFormat.class;
    private static final Class<? extends OutputFormat> INTERMEDIATE_OUTPUT_FORMAT = SequenceFileOutputFormat.class;

    protected abstract Logger getLog();

    protected AbstractHadoopCompiler(final HadoopGraph graph) {
        this.graph = graph;
        this.setConf(new Configuration(this.graph.getConf()));
        jobs = new ArrayList<Job>();
    }

    protected abstract String getMapReduceJarConfigKey();
    protected abstract String getDefaultMapReduceJar();

    @Override
    public void composeJobs() throws IOException {

        if (jobs.size() == 0) {
            return;
        }

        if (getTitanConf().get(TitanHadoopConfiguration.PIPELINE_TRACK_PATHS))
            getLog().warn("Path tracking is enabled for this Titan/Hadoop job (space and time expensive)");
        if (getTitanConf().get(TitanHadoopConfiguration.PIPELINE_TRACK_STATE))
            getLog().warn("State tracking is enabled for this Titan/Hadoop job (full deletes not possible)");

        String customConfigurer = getTitanConf().has(TitanHadoopConfiguration.CLASSPATH_CONFIGURER) ?
                getTitanConf().get(TitanHadoopConfiguration.CLASSPATH_CONFIGURER) : null;

        JobClasspathConfigurer cpConf = JobClasspathConfigurers.get(
                customConfigurer,
                graph.getConf().get(getMapReduceJarConfigKey()),
                getDefaultMapReduceJar());

        // Create temporary job data directory on the filesystem
        Path tmpPath = graph.getJobDir();
        final FileSystem fs = FileSystem.get(graph.getConf());
        fs.mkdirs(tmpPath);
        getLog().debug("Created " + tmpPath + " on filesystem " + fs);
        final String jobPathPrefix = tmpPath.toString() + "/" + Tokens.JOB;

        //////// CHAINING JOBS TOGETHER

        getLog().info("Configuring " + jobs.size() + " MapReduce job(s)...");

        for (int i = 0; i < jobs.size(); i++) {
            final Job job = jobs.get(i);

            final ModifiableHadoopConfiguration jobFaunusConf = ModifiableHadoopConfiguration.of(job.getConfiguration());

            final Path defaultJobDir = new Path(jobPathPrefix + "-" + i);
            final Path prevJobDir = new Path(jobPathPrefix + "-" + (i - 1));

            for (ConfigOption<Boolean> c : Arrays.asList(TitanHadoopConfiguration.PIPELINE_TRACK_PATHS, TitanHadoopConfiguration.PIPELINE_TRACK_STATE)) {
                jobFaunusConf.set(c, getTitanConf().get(c));
            }
            cpConf.configure(job);

            getLog().info("Configuring [Job " + (i + 1) + "/" + jobs.size() + ": " + job.getJobName() + "]");

            // Configure job inputs
            if (i == 0) {
                job.setInputFormatClass(graph.getGraphInputFormat());
                if (FileInputFormat.class.isAssignableFrom(graph.getGraphInputFormat())) {
                    FileInputFormat.setInputPaths(job, graph.getInputLocation());
                    FileInputFormat.setInputPathFilter(job, NoSideEffectFilter.class);
                }
            } else {
                job.setInputFormatClass(INTERMEDIATE_INPUT_FORMAT);
                FileInputFormat.setInputPaths(job, prevJobDir);
                FileInputFormat.setInputPathFilter(job, NoSideEffectFilter.class);
            }

            // Log the input configuration
            try {
                getLog().debug("Set input format: {}", job.getInputFormatClass());
            } catch (ClassNotFoundException e) {
                getLog().warn("Unable to check input format class on current job");
            }
            for (Path p : FileInputFormat.getInputPaths(job)) {
                getLog().debug("Set input path: {}", p);
            }

            final Path curJobDir;

            // Configure job outputs
            if (i == jobs.size() - 1) {
                LazyOutputFormat.setOutputFormatClass(job, graph.getGraphOutputFormat());
                /* Note that the job's output key class is passed in as both the key and value class on the line
                 * of code following this comment.  I think this was unintentional, but if so, it's an old
                 * mistake (going back at least to 0.4.4, before this codebase was imported into Titan).
                 *
                 * I think this line only appears to work because TextOutputFormat is the only supported
                 * sideeffect format, and because TextOutputFormat ignores the configured key and value classes.
                 * TextOutputFormat checks the actual type of the key and value instances passed to it through
                 * the `write` method.  It doesn't appear to use k/v classes configured on the next line.
                 *
                 * As for the assertion about TextOutputFormat being the only supported sideeffect format, see
                 * ResultHookClosure.  It just assumes that a TextFileLineIterator run over the sideeffect output
                 * files will produce readable output.  So, while it's possible to modify this named output so
                 * that the value type makes sense, and that in turn allows using SequenceFileOutputFormat for
                 * sideeffects, doing so prints a bunch of gibberish when running jobs with sideeffect emission
                 * via gremlin.sh.  It's possible that binary sideeffect formats were intended only for
                 * non-interactive execution, but in that case, I would expect ResultHookClosure to check the
                 * job configuration and refrain from running a TextFileLineIterator if it finds a non-Text
                 * sideeffect configuration, as opposed to the current unconditional behavior.
                 */
                addNamedOutput(job, Tokens.SIDEEFFECT, graph.getSideEffectOutputFormat(), job.getOutputKeyClass(), job.getOutputKeyClass());
                //addNamedOutput(job, Tokens.SIDEEFFECT, graph.getSideEffectOutputFormat(), job.getOutputKeyClass(), job.getOutputValueClass());
                addNamedOutput(job, Tokens.GRAPH, graph.getGraphOutputFormat(), NullWritable.class, FaunusVertex.class);
                curJobDir = jobFaunusConf.has(TitanHadoopConfiguration.FINAL_OUTPUT_LOCATION) ?
                        new Path(jobFaunusConf.get(TitanHadoopConfiguration.FINAL_OUTPUT_LOCATION)) : defaultJobDir;

            } else {
                LazyOutputFormat.setOutputFormatClass(job, INTERMEDIATE_OUTPUT_FORMAT);
                addNamedOutput(job, Tokens.SIDEEFFECT, graph.getSideEffectOutputFormat(), job.getOutputKeyClass(), job.getOutputKeyClass());
                //addNamedOutput(job, Tokens.SIDEEFFECT, graph.getSideEffectOutputFormat(), job.getOutputKeyClass(), job.getOutputValueClass());
                addNamedOutput(job, Tokens.GRAPH, INTERMEDIATE_OUTPUT_FORMAT, NullWritable.class, FaunusVertex.class);
                curJobDir = defaultJobDir;
            }
            // Apply the output path to the job's config
            SequenceFileOutputFormat.setOutputPath(job, curJobDir);

            // Log the output format
            try {
                getLog().debug("Set output format: {}", job.getOutputFormatClass());
            } catch (ClassNotFoundException e) {
                getLog().warn("Unable to check output format class on job {}", job);
            }

            // Log the output path
            getLog().debug("Output path: {}", curJobDir);

            // Log the job's key and value classes
            getLog().debug("Map output key class: " + job.getMapOutputKeyClass());
            getLog().debug("Map output val class: " + job.getMapOutputValueClass());
            getLog().debug("Job output key class: " + job.getOutputKeyClass());
            getLog().debug("Job output val class: " + job.getOutputValueClass());
        }

        getLog().info("Configured {} MapReduce job(s)", jobs.size());
    }

    @Override
    public int run(final String[] args) throws Exception {

        final FileSystem fs = FileSystem.get(getConf());

        if (null != graph.getJobDir() &&
                graph.getJobDirOverwrite() &&
                fs.exists(graph.getJobDir())) {
            fs.delete(graph.getJobDir(), true);
        }

        composeJobs();

        final String jobTmp = graph.getJobDir().toString() + "/" + Tokens.JOB;

        getLog().info("Preparing to execute {} MapReduce job(s)...", jobs.size());

        for (int i = 0; i < jobs.size(); i++) {
            final Job job = jobs.get(i);
            try {
                ((JobConfigurationFormat) (FormatTools.getBaseOutputFormatClass(job).newInstance())).updateJob(job);
            } catch (final Exception e) {
            }
            final String jobString = "[Job " + (i + 1) + "/" + jobs.size() + ": " + job.getJobName() + "]";

            getLog().info("Executing " + jobString);
            boolean success = job.waitForCompletion(true);
            if (i > 0) {
                Preconditions.checkNotNull(jobTmp);
                final Path path = new Path(jobTmp + "-" + (i - 1));
                // delete previous intermediate graph data
                for (final FileStatus temp : fs.globStatus(new Path(path.toString() + "/" + Tokens.GRAPH + "*"))) {
                    getLog().debug("Deleting temp data location: " + temp.getPath());
                    fs.delete(temp.getPath(), true);
                }
                // delete previous intermediate graph data
                for (final FileStatus temp : fs.globStatus(new Path(path.toString() + "/" + Tokens.PART + "*"))) {
                    getLog().debug("Deleting temp data location: " + temp.getPath());
                    fs.delete(temp.getPath(), true);
                }
            }
            if (!success) {
                getLog().error("Error executing {}; this job has failed and {} subsequent MapReduce job(s) have been canceled",
                        jobString, jobs.size() - (i + 1));
                return -1;
            } else {
                getLog().info("Executed {} successfully", jobString);
            }
        }
        getLog().info("Finished executing {} MapReduce job(s)", jobs.size());
        return 0;
    }

    private void addNamedOutput(Job job, String name, Class<? extends OutputFormat> outfmt, Class<?> keyClass, Class<?> valueClass) {
        getLog().debug("Adding output: name={}, format={}, keycls={}, valcls={}", name, outfmt, keyClass, valueClass);
        MultipleOutputs.addNamedOutput(job, name, outfmt, keyClass, valueClass);
    }
}
