package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin;

import com.tinkerpop.gremlin.groovy.Gremlin;
import com.tinkerpop.gremlin.groovy.console.ErrorHookClosure;
import com.tinkerpop.gremlin.groovy.console.NullResultHookClosure;
import com.tinkerpop.gremlin.groovy.console.PromptClosure;
import jline.History;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO;
import org.codehaus.groovy.tools.shell.InteractiveShellRunner;
import org.codehaus.groovy.tools.shell.util.Preferences;

import java.io.*;
import java.nio.charset.Charset;
import java.util.prefs.PreferenceChangeEvent;
import java.util.prefs.PreferenceChangeListener;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Console {

    private static final String HISTORY_FILE = ".gremlin_titan_hadoop_history";
    private static final String STANDARD_INPUT_PROMPT = "gremlin> ";
    private static final String STANDARD_RESULT_PROMPT = "==>";

    /*static {
      try {
            System.setProperty("log4j.configuration", "./resources" + File.separatorChar + "log4j.properties");
        } catch (Exception e) {
        }
    }*/

    public Console(final IO io, final String inputPrompt, final String resultPrompt, String cliArgs[]) {
        io.out.println();
        io.out.println("         \\,,,/");
        io.out.println("         (o o)");
        io.out.println("-----oOOo-(_)-oOOo-----");

        // Evaluate imports
        final Groovysh groovy = new Groovysh();
        groovy.setResultHook(new NullResultHookClosure(groovy));
        for (final String imps : Imports.getImports()) {
            groovy.execute("import " + imps);
        }
        for (final String evs : Imports.getEvaluates()) {
            groovy.execute(evs);
        }

        // Instantiate console objects: the ResultHook, History handler, ErrorHook, ConsolePreferenceChangeListener and InteractiveShellRunner
        ConsolePreferenceChangeListener prefListener = new ConsolePreferenceChangeListener();
        Preferences.addChangeListener(prefListener);

        ResultHookClosure resultHook = new ResultHookClosure(groovy, io, resultPrompt);
        resultHook.setConsolePreferenceConsumers(prefListener);
        groovy.setResultHook(resultHook);
        groovy.setHistory(new History());

        final InteractiveShellRunner runner = new InteractiveShellRunner(groovy, new PromptClosure(groovy, inputPrompt));
        runner.setErrorHandler(new ErrorHookClosure(runner, io));
        try {
            runner.setHistory(new History(new File(System.getProperty("user.home") + "/" + HISTORY_FILE)));
        } catch (IOException e) {
            io.err.println("Unable to create history file: " + HISTORY_FILE);
        }

        // Define convenience methods using metaClasses
        Gremlin.load();
        HadoopGremlin.load();

        // Evaluate arguments
        if (null != cliArgs && 0 < cliArgs.length) {
            initializeShellWithScript(io, cliArgs[0], groovy);
        }

        // Enter the REPL
        try {
            runner.run();
        } catch (Error e) {
            //System.err.println(e.getMessage());
        }

        System.exit(0);
    }

    public Console() {
        this(new IO(System.in, System.out, System.err), STANDARD_INPUT_PROMPT, STANDARD_RESULT_PROMPT, new String[]{});
    }

    public Console(String cliArgs[]) {
        this(new IO(System.in, System.out, System.err), STANDARD_INPUT_PROMPT, STANDARD_RESULT_PROMPT, cliArgs);
    }

    public static void main(final String[] args) {
        new Console(args);
    }

    private void initializeShellWithScript(final IO io, final String initScriptFile, final Groovysh groovy) {
        if (initScriptFile != null) {
            String line = "";
            try {
                final BufferedReader reader = new BufferedReader(new InputStreamReader(
                        new FileInputStream(initScriptFile), Charset.forName("UTF-8")));
                while ((line = reader.readLine()) != null) {
                    groovy.execute(line);
                }

                reader.close();
            } catch (FileNotFoundException fnfe) {
                io.err.println(String.format("Gremlin initialization file not found at [%s].", initScriptFile));
                System.exit(1);
            } catch (IOException ioe) {
                io.err.println(String.format("Bad line in Gremlin initialization file at [%s].", line));
                System.exit(1);
            }
        }
    }
}