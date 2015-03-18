package com.thinkaurelius.titan.util.encoding;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;

import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ConversionHelper {

    public static int getTTLSeconds(Duration duration) {
        if (duration == null) {
            return 0;
        }
        long ttlSeconds = Math.max(0, duration.getLength(TimeUnit.SECONDS));
        Preconditions.checkArgument(ttlSeconds <= Integer.MAX_VALUE, "tll value is too large [%s] - value overflow", duration);
        return (int) ttlSeconds;
    }

    public static int getTTLSeconds(long time, TimeUnit unit) {
        return getTTLSeconds(new StandardDuration(time, unit));
    }

}
