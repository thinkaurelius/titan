package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin;

import com.thinkaurelius.titan.hadoop.HadoopPipeline;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.hdfs.HDFSTools;
import com.thinkaurelius.titan.hadoop.hdfs.TextFileLineIterator;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.transform.ToStringPipe;
import com.tinkerpop.pipes.util.iterators.SingleIterator;

import groovy.lang.Closure;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.groovy.tools.shell.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ResultHookClosure extends Closure {

    private static final Logger log =
            LoggerFactory.getLogger(ResultHookClosure.class);

    /**
     * This environment variable is likely to be changed or removed in future
     * versions of Titan, particularly Titan releases based on TinkerPop 3 and later.
     *
     * This would ideally be handled by the Console main class using JCommander
     * or commons-cli or args4j etc to parse its main-method arguments instead of
     * relying on an environment variable, but I don't want to rewrite Console's
     * argument handling for one option.  Looking for a tipping point.
     *
     * Do not assume clean upgrades of scripts that rely on setting this
     * environment variable directly to control the shell' collection enumeration limit.
     */
    private static final String LINES_ENV_VAR = "TITAN_RESULT_WINDOW_SIZE";
    private static final int LINES_MIN = 1;
    private static final int LINES;

    private final String resultPrompt;
    private final IO io;

    static {
        int l = 15;

        /* If this code contains an overlooked exception throw site, and that
         * exception propagates out of the static initializer, then this class
         * will fail to init and the Console will refuse to start, even though
         * we're just trying to read a silly cosmetic variable.  Catching
         * Throwable out of paranoia.
         */
        try {
            String raw = System.getenv(LINES_ENV_VAR);
            if (null != raw) {
                try {
                    l = Integer.parseInt(raw);
                    log.debug("Read environment variable {}={}", LINES_ENV_VAR, l);
                    if (l < LINES_MIN) {
                        log.debug("Configured value {} for {} is too low; increasing to minimum value {}",
                                l, LINES_ENV_VAR, LINES_MIN);
                        l = LINES_MIN;
                    }
                } catch (NumberFormatException e) {
                    log.warn("Unable to parse environment variable {}={}", LINES_ENV_VAR, raw, e);
                }
            }
        } catch (Throwable t) {
            log.warn("Could not read environment variable {}", LINES_ENV_VAR);
        }

        LINES = l;
    }

    public ResultHookClosure(final Object owner, final IO io, final String resultPrompt) {
        super(owner);
        this.io = io;
        this.resultPrompt = resultPrompt;
    }

    public Object call(final Object[] args) {
        final Object result = args[0];
        final Iterator itty;
        if (result instanceof HadoopPipeline) {
            try {
                final HadoopPipeline pipeline = (HadoopPipeline) result;
                pipeline.submit();
                final FileSystem hdfs = FileSystem.get(pipeline.getGraph().getConf());
                final Path output = HDFSTools.getOutputsFinalJob(hdfs, pipeline.getGraph().getJobDir().toString());
                itty = new TextFileLineIterator(hdfs, hdfs.globStatus(new Path(output.toString() + "/" + Tokens.SIDEEFFECT + "*")), LINES + 1);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } else {
            itty = new ToStringPipe();
            ((Pipe) itty).setStarts(new SingleIterator<Object>(result));
        }

        int linesPrinted = 0;
        while (itty.hasNext() && linesPrinted < LINES) {
            this.io.out.println(this.resultPrompt + itty.next());
            linesPrinted++;
        }
        if (linesPrinted == LINES && itty.hasNext())
            this.io.out.println(this.resultPrompt + "...");

        return null;
    }
}