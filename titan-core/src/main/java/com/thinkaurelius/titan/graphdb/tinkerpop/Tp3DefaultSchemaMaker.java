package com.thinkaurelius.titan.graphdb.tinkerpop;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.schema.DefaultSchemaMaker;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class Tp3DefaultSchemaMaker implements DefaultSchemaMaker {

    public static final DefaultSchemaMaker INSTANCE = new Tp3DefaultSchemaMaker();

    private Tp3DefaultSchemaMaker() {
    }

    @Override
    public Cardinality defaultPropertyCardinality(String key) {
        return Cardinality.LIST;
    }

    @Override
    public boolean ignoreUndefinedQueryTypes() {
        return true;
    }

}
