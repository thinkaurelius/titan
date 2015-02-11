package com.thinkaurelius.titan.hadoop.tinkerpop.gremlin;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.codehaus.groovy.tools.shell.util.Preferences;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.prefs.PreferenceChangeEvent;
import java.util.prefs.PreferenceChangeListener;

public class ConsolePreferenceChangeListener implements PreferenceChangeListener {

    static final String PREF_TINKERPOP_PREFIX = "tp-";

    private static final Logger log =
            LoggerFactory.getLogger(ConsolePreferenceChangeListener.class);

    private final ConcurrentHashMap<String, Function<PreferenceChangeEvent, ?>> prefChangeConsumers =
            new ConcurrentHashMap<String, Function<PreferenceChangeEvent, ?>>();

    /**
     * Add a new console preference consumer, and, if the supplied key maps to
     * a non-null console preference value, immediately fire a change event
     *
     *
     * @param triggerPrefKey
     * @param consumer
     */
    public void setConsumer(String triggerPrefKey, Function<PreferenceChangeEvent, ?> consumer) {

        Preconditions.checkNotNull(triggerPrefKey);
        // Preferences javadoc mandates that no path contain successive slashes
        Preconditions.checkArgument(!triggerPrefKey.startsWith("/"));

        String k = PREF_TINKERPOP_PREFIX + triggerPrefKey;

        Function<?, ?> oldConsumer = prefChangeConsumers.putIfAbsent(k, consumer);

        if (null == oldConsumer) {
            log.debug("Installing new preference consumer for key {}", k);
        } else {
            log.debug("Replacing existing preference consumer for key {} (old consumer: {})",
                    k, oldConsumer);
        }

        String currentValue = Preferences.get(k);
        if (null != currentValue) {
            log.debug("Resetting stored value to trigger consumer: {}={}", k, currentValue);
            Preferences.put(k, currentValue);
        } else {
            log.debug("Read null for {}", k);
        }
    }

    @Override
    public void preferenceChange(PreferenceChangeEvent evt) {
        // This is probably never null, but why not check
        if (null == evt || null == evt.getKey())
            return;

        String k = evt.getKey();

        Function<PreferenceChangeEvent, ?> consumer = prefChangeConsumers.get(k);

        if (null == consumer) {
            log.debug("Ignoring preference key {} (no consumer registered)", k);
            return;
        }

        log.debug("Invoking consumer {} for key {}", consumer, k);

        consumer.apply(evt); // TODO uncaught exception handling?
    }
}
