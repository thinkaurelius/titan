package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.schema.RelationTypeDefinition;
import com.thinkaurelius.titan.graphdb.types.system.EmptyRelationType;
import com.tinkerpop.blueprints.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class FaunusRelationType extends EmptyRelationType implements InternalRelationType {

    private final RelationTypeDefinition definition;
    private final boolean isHidden;
    private final boolean isUnchecked;

    protected FaunusRelationType(RelationTypeDefinition def, boolean hidden) {
        this(def, hidden, false);
    }

    protected FaunusRelationType(RelationTypeDefinition def, boolean hidden, boolean isUnchecked) {
        this.definition = def;
        this.isHidden = hidden;
        this.isUnchecked = isUnchecked;
    }

    public boolean isUnchecked() {
        return isUnchecked;
    }

    @Override
    public String getName() {
        return definition.getName();
    }

    @Override
    public long getLongId() {
        return definition.getLongId();
    }

    @Override
    public boolean hasId() {
        return true;
    }

    @Override
    public void setId(long id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConsistencyModifier getConsistencyModifier() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Multiplicity getMultiplicity() {
        return definition.getMultiplicity();
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return definition.isUnidirected(dir);
    }

    @Override
    public boolean isHiddenType() {
        return isHidden;
    }

    @Override
    public String toString() {
        return definition.getName();
    }

    @Override
    public int hashCode() {
        return definition.getName().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null || !(other instanceof RelationType)) return false;
        return definition.getName().equals(((RelationType)other).getName());
    }

}
