package com.thinkaurelius.titan.tinkerpop.rexster;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.rexster.config.hinted.ElementRange;

public class ByteElementRange<E extends Element> implements ElementRange<StaticBuffer, E> {

    private static final long serialVersionUID = 1L;

    // This doesn't really need to be static, but it should be final
    public static final int MIN_BYTES = 4;

    private final Class<E> token;
    private final StaticBuffer startIncl;
    private final StaticBuffer endExcl;
    private final boolean wrapped;
    private final int priority;

    /**
     * Both StaticBuffer objects must implement Serializable and must be at
     * least {@value #MIN_BYTES} bytes long.
     *
     * @param token
     *            the type token for the graph elements
     * @param startIncl
     *            lower bound, inclusive; only the first {@value #MIN_BYTES} are considered
     * @param endExcl
     *            upper bound, exclusive; only the first {@value #MIN_BYTES} are considered
     * @param priority
     *            priority
     */
    public ByteElementRange(Class<E> token, StaticBuffer startIncl, StaticBuffer endExcl, int priority) {
        this.token     = token;
        this.startIncl = truncate(startIncl);
        this.endExcl   = truncate(endExcl);
        this.priority  = priority;

        Preconditions.checkNotNull(this.token);
        Preconditions.checkArgument(MIN_BYTES == this.startIncl.length());
        Preconditions.checkArgument(MIN_BYTES == this.endExcl.length());

        wrapped = ByteBufferUtil.isSmallerOrEqualThan(this.endExcl, this.startIncl);
    }

    @Override
    public Class<E> getElementType() {
        return token;
    }

    @Override
    public boolean contains(StaticBuffer item) {
        if (!wrapped) {
            return ByteBufferUtil.isSmallerThan(item, endExcl) &&
                   ByteBufferUtil.isSmallerOrEqualThan(startIncl, item);
        } else {
            return ByteBufferUtil.isSmallerThan(item, endExcl) ||
                   ByteBufferUtil.isSmallerOrEqualThan(startIncl, item);
        }
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((endExcl == null) ? 0 : endExcl.hashCode());
        result = prime * result + ((startIncl == null) ? 0 : startIncl.hashCode());
        result = prime * result + ((token == null) ? 0 : token.hashCode());
        result = prime * result + priority;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        ByteElementRange<?> other = (ByteElementRange<?>) obj;

        if (endExcl == null) {
            if (other.endExcl != null)
                return false;
        } else if (!ByteBufferUtil.equals(endExcl, other.endExcl)) {
            return false;
        }

        if (startIncl == null) {
            if (other.startIncl != null)
                return false;
        } else if (!ByteBufferUtil.equals(startIncl, other.startIncl)) {
            return false;
        }

        if (token == null) {
            if (other.token != null)
                return false;
        } else if (!token.equals(other.token)) {
            return false;
        }

        if (priority != other.priority)
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "ByteElementRange[token=" + token + ", startIncl=" + startIncl
                + ", endExcl=" + endExcl + ", wrapped=" + wrapped
                + ", priority=" + priority + "]";
    }

    private StaticBuffer truncate(StaticBuffer s) {
        if (s.length() == MIN_BYTES)
            return s;

        assert MIN_BYTES < s.length();

        byte raw[] = new byte[MIN_BYTES];

        for (int i = 0; i < raw.length; i++)
            raw[i] = s.getByte(i);

        return new StaticArrayBuffer(raw);
    }
}
