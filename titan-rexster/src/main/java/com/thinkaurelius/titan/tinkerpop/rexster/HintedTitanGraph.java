package com.thinkaurelius.titan.tinkerpop.rexster;

import java.util.ArrayList;
import java.util.List;

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

        /*
         * Don't return Lists.transform(...) here.
         *
         * The object returned by Lists.transform(...) retains a reference to
         * the KeyRange named local. This reference is not transient. Java
         * serialization will die on this reference when attempting to send our
         * edge/vertex ranges over JGroups because KeyRange is not
         * serializable.
         *
         * We could make KeyRange serializable. However, it doesn't really need
         * to be serializable. It's an implementation detail that is totally
         * irrelevant from the perspective of a HintedGraph interface user.
         *
         * This is why we go out of our way to return a standard-library
         * ArrayList instead of just returning Lists.transform(...).
         */
        //return Lists.transform(local, new KeyRangeToElementRange<E>(token));

        ArrayList<ElementRange<StaticBuffer, E>> result =
                new ArrayList<ElementRange<StaticBuffer, E>>(local.size());
        result.addAll(Lists.transform(local, new KeyRangeToElementRange<E>(token)));
        return result;
    }
    
    @Override
    public String toString() {
        return "HintedTitanGraph[vertexRanges=" + vertexRanges
                + ", edgeRanges=" + edgeRanges + "]";
    }

}