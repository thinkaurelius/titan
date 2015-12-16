package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.PropertyKey;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface MixedIndexType extends IndexType {

    @Override
    public ParameterIndexField[] getFieldKeys();

    @Override
    public ParameterIndexField getField(PropertyKey key);

    public String getStoreName();

}
