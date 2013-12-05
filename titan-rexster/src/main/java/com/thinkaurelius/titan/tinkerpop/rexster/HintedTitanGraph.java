package com.thinkaurelius.titan.tinkerpop.rexster;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.rexster.config.hinted.ElementRange;
import com.tinkerpop.rexster.config.hinted.HintedGraph;

public class HintedTitanGraph implements HintedGraph<StaticBuffer> {
    
    private static final long serialVersionUID = -6777982876256742108L;
    
    private volatile List<ElementRange<StaticBuffer, Vertex>> vertexRanges;
    private volatile List<ElementRange<StaticBuffer, Edge>> edgeRanges;
    private transient StandardTitanGraph g;
        
    public HintedTitanGraph(StandardTitanGraph g) {
        vertexRanges = keyRangeToElementRange(g.getEdgeStoreLocalKeyPartition(), Vertex.class);
        edgeRanges = keyRangeToElementRange(g.getEdgeStoreLocalKeyPartition(), Edge.class);
    }

    @Override
    public List<ElementRange<StaticBuffer, Vertex>> getVertexRanges() {
        if (null != g)
            vertexRanges = keyRangeToElementRange(g.getEdgeStoreLocalKeyPartition(), Vertex.class);
        
        return vertexRanges; 
    }

    @Override
    public List<ElementRange<StaticBuffer, Edge>> getEdgeRanges() {
        if (null != g)
            edgeRanges = keyRangeToElementRange(g.getEdgeStoreLocalKeyPartition(), Edge.class);
        
        return edgeRanges;
    }
    
    private <E extends Element> List<ElementRange<StaticBuffer, E>> keyRangeToElementRange(final List<KeyRange> local, final Class<E> token) {
        
        return Lists.transform(local, new KeyRangeToElementRange<E>(token));
    }
}
