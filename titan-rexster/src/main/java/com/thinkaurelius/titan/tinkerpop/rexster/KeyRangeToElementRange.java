package com.thinkaurelius.titan.tinkerpop.rexster;

import java.io.Serializable;

import com.google.common.base.Function;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
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
        return new ElementRange<StaticBuffer, E>(token, input.getStart(),
                input.getEnd(), ELEMENT_RANGE_PRIORITY);
    }

}
