package com.thinkaurelius.titan.tinkerpop.rexster;

import java.io.Serializable;

import com.google.common.base.Function;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.rexster.config.hinted.ElementRange;

public class KeyRangeToElementRange<E extends Element> implements
        Function<KeyRange, ElementRange<StaticBuffer, E>>, Serializable {

    private static final long serialVersionUID = 152689113565125072L;

    public static final int ELEMENT_RANGE_PRIORITY = 1;

    private final Class<E> token;

    public KeyRangeToElementRange(Class<E> token) {
        this.token = token;
    }

    @Override
    public ElementRange<StaticBuffer, E> apply(KeyRange input) {

        /*
         * Make start and end buffers StaticArrayBuffer. This class is
         * serializable. Other implementations of the StaticBuffer interface
         * (e.g. StaticByteBuffer) may not necessarily be serializable.
         */

        StaticBuffer start = new StaticArrayBuffer(input.getStart());
        StaticBuffer end   = new StaticArrayBuffer(input.getEnd());

        return new ByteElementRange<E>(token, start, end, ELEMENT_RANGE_PRIORITY);
    }

}
