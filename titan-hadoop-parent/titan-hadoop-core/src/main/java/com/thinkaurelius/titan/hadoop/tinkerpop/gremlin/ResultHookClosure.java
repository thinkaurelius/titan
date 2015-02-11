package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin;

import com.google.common.base.Function;
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
import java.util.prefs.PreferenceChangeEvent;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ResultHookClosure extends Closure {

    private static final Logger log =
            LoggerFactory.getLogger(ResultHookClosure.class);

    private final String resultPrompt;
    private final IO io;

    // Defaults established in #924
    private static final int OLAP_PEEK_DEFAULT = 50;
    private static final int OLTP_PEEK_DEFAULT = 500;

    private int olapPeek = OLAP_PEEK_DEFAULT;
    private int oltpPeek = OLTP_PEEK_DEFAULT;

    public ResultHookClosure(final Object owner, final IO io, final String resultPrompt) {
        super(owner);
        this.io = io;
        this.resultPrompt = resultPrompt;
    }

    public Object call(final Object[] args) {
        final Object result = args[0];
        final Iterator itty;
        final int peekLines;

        if (result instanceof HadoopPipeline) {
            peekLines = olapPeek;

            if (0 == peekLines)
                return null;

            try {
                final HadoopPipeline pipeline = (HadoopPipeline) result;
                pipeline.submit();
                final FileSystem hdfs = FileSystem.get(pipeline.getGraph().getConf());
                final Path output = HDFSTools.getOutputsFinalJob(hdfs, pipeline.getGraph().getJobDir().toString());
                // Avoid overflow; olapPeek will be Integer.MAX_VALUE if user asked for -1
                // The point of +1 is to let us tell whether we ran out of lines or hit the peek limit
                int peekPlusOne = peekLines == Integer.MAX_VALUE ? Integer.MAX_VALUE : peekLines + 1;
                itty = new TextFileLineIterator(hdfs, hdfs.globStatus(new Path(output.toString() + "/" + Tokens.SIDEEFFECT + "*")), peekPlusOne);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } else {
            peekLines = oltpPeek;

            if (0 == peekLines)
                return null;

            itty = new ToStringPipe();
            ((Pipe) itty).setStarts(new SingleIterator<Object>(result));
        }

        int linesPrinted = 0;
        while (itty.hasNext() && linesPrinted < peekLines) {
            this.io.out.println(this.resultPrompt + itty.next());
            linesPrinted++;
        }
        if (linesPrinted == peekLines && itty.hasNext())
            this.io.out.println(this.resultPrompt + "...");

        return null;
    }

    void setConsolePreferenceConsumers(ConsolePreferenceChangeListener listener) {
        listener.setConsumer("olap-result-peek", new OLAPPeekConsumer());
        listener.setConsumer("oltp-result-peek", new OLTPPeekConsumer());
    }

    private int safeIntConversion(String raw, int defaultValue) {
        int l = defaultValue;

        try {
            if (null != raw) {
                try {
                    l = Integer.parseInt(raw);
                    log.debug("Parsed integer {} (original string: \"{}\")", l, raw);
                    if (l < 0) {
                        log.debug("Overwriting negative integer {} with {}", l, Integer.MAX_VALUE);
                        l = Integer.MAX_VALUE;
                    }
                } catch (NumberFormatException e) {
                    log.warn("String {} could not be converted to a number", raw, e);
                }
            }
        } catch (Throwable t) {
            log.warn("Error parsing {}", raw, t);
        }

        return l;
    }

    private class OLTPPeekConsumer implements Function<PreferenceChangeEvent, Void> {
        @Override
        public Void apply(PreferenceChangeEvent input) {
            oltpPeek = safeIntConversion(input.getNewValue(), OLTP_PEEK_DEFAULT);
            return null;
        }
    }

    private class OLAPPeekConsumer implements Function<PreferenceChangeEvent, Void> {
        @Override
        public Void apply(PreferenceChangeEvent input) {
            olapPeek = safeIntConversion(input.getNewValue(), OLAP_PEEK_DEFAULT);
            return null;
        }
    }
}