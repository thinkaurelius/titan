package com.thinkaurelius.titan.graphdb.serializer;

import java.io.Serializable;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class SpecialInt implements Serializable {

	private static final long serialVersionUID = -3140817057952829487L;

	private int value;

    public SpecialInt(int value) {
        this.value=value;
    }

    public int getValue() {
        return value;
    }

}
