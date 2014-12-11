package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.graphdb.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class DefaultSchemaProvider implements SchemaProvider {

    private static final Logger log = LoggerFactory.getLogger(DefaultSchemaProvider.class);

    public static final DefaultSchemaProvider INSTANCE = new DefaultSchemaProvider();

    private DefaultSchemaProvider() {}

    @Override
    public EdgeLabelDefinition getEdgeLabel(String name) {
        log.debug("Creating default edge label definition for {}", name);
        return new EdgeLabelDefinition(name, FaunusElement.NO_ID, Multiplicity.MULTI, false);
    }

    @Override
    public PropertyKeyDefinition getPropertyKey(String name) {
        log.debug("Creating default property key definition for {}", name);
        return new PropertyKeyDefinition(name, FaunusElement.NO_ID, Cardinality.SINGLE, Object.class);
    }

    @Override
    public RelationTypeDefinition getRelationType(String name) {
        log.debug("Forced null relation type {}", name, new RuntimeException());
        return null;
    }

    @Override
    public VertexLabelDefinition getVertexLabel(String name) {
        log.debug("Creating default vertex label definition for {}", name);
        return new VertexLabelDefinition(name, FaunusElement.NO_ID,false,false);
    }

    public static SchemaProvider asBackupProvider(final SchemaProvider provider) {
        return asBackupProvider(provider,INSTANCE);
    }

    public static SchemaProvider asBackupProvider(final SchemaProvider provider, final SchemaProvider backup) {
        return new SchemaProvider() {
            @Override
            public EdgeLabelDefinition getEdgeLabel(String name) {
                EdgeLabelDefinition def = provider.getEdgeLabel(name);
                if (def!=null) return def;
                else return backup.getEdgeLabel(name);
            }

            @Override
            public PropertyKeyDefinition getPropertyKey(String name) {
                PropertyKeyDefinition def = provider.getPropertyKey(name);
                if (def!=null) return def;
                else return backup.getPropertyKey(name);
            }

            @Override
            public RelationTypeDefinition getRelationType(String name) {
                RelationTypeDefinition def = provider.getRelationType(name);
                if (def!=null) return def;
                else return backup.getRelationType(name);
            }

            @Override
            public VertexLabelDefinition getVertexLabel(String name) {
                VertexLabelDefinition def = provider.getVertexLabel(name);
                if (def!=null) return def;
                else return backup.getVertexLabel(name);
            }

            @Override
            public String toString() {
                return "DelegatingSchemaProvider[provider=" + provider + ",backup=" + backup + "]";
            }
        };
    }

}
